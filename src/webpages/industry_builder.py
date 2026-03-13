import pandas as pd
import streamlit as st
import sys
import time
from typing import Any, cast

from utils.characters_api import (
    build_character_options,
    build_owned_blueprint_character_corporation_scope_options,
    build_owned_blueprint_character_scope_options,
    fetch_characters,
)
from utils.corporations_api import build_owned_blueprint_corporation_scope_options
from utils.aggrid_formatters import js_eu_number_formatter
from utils.aggrid_import import import_aggrid
from utils.formatters import format_duration
from utils.flask_api import api_get, api_post, cached_api_get
from utils.industry_profiles_api import build_industry_profile_options, fetch_industry_profiles


_ag = import_aggrid()
AgGrid = _ag.AgGrid  # type: ignore
GridOptionsBuilder = _ag.GridOptionsBuilder  # type: ignore
JsCode = _ag.JsCode  # type: ignore
_AGGRID_IMPORT_ERROR = _ag.import_error


def _rerun() -> None:
    st.rerun()


@st.cache_data(ttl=300)
def _fetch_product_overview(
    *,
    force_refresh: bool = False,
    maximize_bp_runs: bool = False,
    build_from_bpc: bool = True,
    have_blueprint_source_only: bool = True,
    include_reactions: bool = False,
    industry_profile_id: int | None = None,
    owned_blueprints_scope: str = "all_characters",
    character_id: int,
) -> list[dict]:
    path = (
        f"/industry_products/{int(character_id)}"
        f"?maximize_bp_runs={1 if maximize_bp_runs else 0}"
        f"&build_from_bpc={1 if build_from_bpc else 0}"
        f"&have_blueprint_source_only={1 if have_blueprint_source_only else 0}"
        f"&include_reactions={1 if include_reactions else 0}"
        f"&owned_blueprints_scope={owned_blueprints_scope}"
    )
    if industry_profile_id is not None and int(industry_profile_id) > 0:
        path += f"&industry_profile_id={int(industry_profile_id)}"
    if force_refresh:
        path += "&refresh=1"
    resp = api_get(path, timeout_seconds=120 if force_refresh else 60) or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to load industry product overview")
    data = resp.get("data") or []
    return data if isinstance(data, list) else []


@st.cache_data(ttl=30)
def _fetch_job_manager_status() -> dict:
    resp = api_get("/industry_job_manager/status") or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to load industry job manager status")
    data = resp.get("data") or {}
    return data if isinstance(data, dict) else {}


def _start_product_overview_refresh(
    *,
    maximize_bp_runs: bool,
    build_from_bpc: bool,
    have_blueprint_source_only: bool,
    include_reactions: bool,
    industry_profile_id: int | None,
    owned_blueprints_scope: str,
    character_id: int,
) -> dict[str, Any]:
    resp = api_post(
        f"/industry_products/{int(character_id)}/refresh",
        {
            "force_refresh": True,
            "maximize_bp_runs": bool(maximize_bp_runs),
            "build_from_bpc": bool(build_from_bpc),
            "have_blueprint_source_only": bool(have_blueprint_source_only),
            "include_reactions": bool(include_reactions),
            "industry_profile_id": int(industry_profile_id) if industry_profile_id is not None else None,
            "owned_blueprints_scope": str(owned_blueprints_scope),
        },
    ) or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to start industry product overview refresh")
    data = resp.get("data") or {}
    return data if isinstance(data, dict) else {}


def _fetch_product_overview_refresh_status(job_id: str) -> dict[str, Any]:
    resp = api_get(f"/industry_products/refresh/{job_id}", timeout_seconds=30) or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to load industry product overview refresh status")
    data = resp.get("data") or {}
    return data if isinstance(data, dict) else {}


def _get_manufacturing_job(row: dict[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], row.get("manufacturing_job") or {})


def _get_product_name(row: dict[str, Any]) -> str:
    return str(row.get("type_name") or "")


def _get_product_group_name(row: dict[str, Any]) -> str:
    return str(row.get("group_name") or "")


def _get_product_category_name(row: dict[str, Any]) -> str:
    return str(row.get("category_name") or "")


def _get_product_quantity(row: dict[str, Any]) -> int:
    return int(row.get("quantity") or 0)


def _get_effective_runs(row: dict[str, Any]) -> int:
    return int(_get_manufacturing_job(row).get("runs") or 0)


def _get_skill_requirements_met(row: dict[str, Any]) -> bool:
    skills = _get_manufacturing_job(row).get("skills") or {}
    if not isinstance(skills, dict):
        return False
    return bool(skills.get("skill_requirements_met", False))


def _format_blueprint_owner_source(asset: dict[str, Any]) -> str:
    if not isinstance(asset, dict) or not asset:
        return ""
    character_name = str(asset.get("character_name") or "").strip()
    corporation_name = str(asset.get("corporation_name") or "").strip()
    if character_name and corporation_name:
        return f"{character_name} + {corporation_name}"
    if character_name:
        return character_name
    if corporation_name:
        return corporation_name
    owner_type = str(asset.get("owner_type") or "").strip().lower()
    if owner_type == "character":
        owner_id = int(asset.get("character_id") or 0)
        return f"character {owner_id}" if owner_id > 0 else "character"
    if owner_type == "corporation":
        owner_id = int(asset.get("corporation_id") or 0)
        return f"corporation {owner_id}" if owner_id > 0 else "corporation"
    character_id = int(asset.get("character_id") or 0)
    if character_id > 0:
        return f"character {character_id}"
    corporation_id = int(asset.get("corporation_id") or 0)
    if corporation_id > 0:
        return f"corporation {corporation_id}"
    return ""


def _get_blueprint_copy_source(row: dict[str, Any]) -> str:
    return _format_blueprint_owner_source(_get_manufacturing_job(row).get("blueprint_copy") or {})


def _get_blueprint_original_source(row: dict[str, Any]) -> str:
    return _format_blueprint_owner_source(_get_manufacturing_job(row).get("blueprint_original") or {})


@st.cache_data(ttl=3600)
def _fetch_solar_system_security_map() -> dict[int, float]:
    resp = cached_api_get("/solar_systems") or {}
    if resp.get("status") != "success":
        raise RuntimeError(resp.get("message") or "Failed to load solar systems")

    data = resp.get("data") or []
    out: dict[int, float] = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        try:
            system_id = int(entry.get("id") or 0)
            security_status = float(entry.get("security_status") or 0.0)
        except Exception:
            continue
        if system_id > 0:
            out[system_id] = security_status
    return out


def _get_meta_group_name(row: dict[str, Any]) -> str:
    raw_name = str(row.get("meta_group_name") or "").strip()
    normalized = raw_name.lower()
    if normalized in {"tech i", "structure tech i", "abyssal"}:
        return "Tech I"
    if normalized in {"tech ii", "structure tech ii"}:
        return "Tech II"
    if normalized in {"tech iii", "structure tech iii"}:
        return "Tech III"
    if normalized in {"faction", "structure faction"}:
        return "Faction"
    if normalized in {"storyline", "limited time"}:
        return "Storyline"
    return raw_name


def _meta_group_label(meta_group_name: str) -> str:
    return meta_group_name if meta_group_name else "No Meta Group"


def _meta_group_toggle_key(meta_group_name: str) -> str:
    normalized = meta_group_name.strip().lower() or "none"
    safe = "".join(ch if ch.isalnum() else "_" for ch in normalized)
    return f"industry_builder_meta_group_{safe}"


def _ordered_meta_group_names(meta_group_names: set[str]) -> list[str]:
    preferred_order = [
        "Tech I",
        "Tech II",
        "Tech III",
        "Faction",
        "Storyline",
        "Officer",
        "No Meta Group",
    ]
    ordered: list[str] = []
    available = {_meta_group_label(name): name for name in meta_group_names}
    for label in preferred_order:
        if label in available:
            ordered.append(available[label])

    remaining = sorted(
        [name for name in meta_group_names if _meta_group_label(name) not in preferred_order],
        key=lambda name: _meta_group_label(name).lower(),
    )
    ordered.extend(remaining)
    return ordered


def _tree_node_type_label(node: dict[str, Any]) -> str:
    node_type = str(node.get("node_type") or "").strip().lower()
    if node_type == "product":
        return "Product"
    if node_type == "activity":
        return "Job"
    if node_type == "materials":
        return "Materials"
    if node_type == "material":
        return "Material"
    return node_type.title() if node_type else "Step"


def _flatten_overview_job_tree_rows(overview_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened_rows: list[dict[str, Any]] = []
    tree_path_separator = "|||"

    def add_node(
        *,
        source_row: dict[str, Any],
        node: dict[str, Any],
        path_ids: list[str],
        sibling_key: str,
        order_key: str,
    ) -> None:
        manufacturing_job = _get_manufacturing_job(source_row)
        node_type = str(node.get("node_type") or "")
        activity = str(node.get("activity") or "")
        duration_seconds = node.get("duration_seconds")
        job_cost = node.get("total_job_cost")
        if job_cost is None:
            job_cost = node.get("job_cost")

        material_cost = None
        total_cost = job_cost
        bpc_source = ""
        bpo_source = ""
        meta_group = ""
        category = ""
        current_path = tree_path_separator.join([*path_ids, sibling_key])

        if node_type == "product":
            material_cost = manufacturing_job.get("material_cost")
            job_cost = manufacturing_job.get("total_job_cost")
            total_cost = manufacturing_job.get("total_cost")
            bpc_source = _get_blueprint_copy_source(source_row)
            bpo_source = _get_blueprint_original_source(source_row)
            meta_group = str(source_row.get("meta_group_name") or "")
            category = _get_product_category_name(source_row)
        elif activity == "manufacturing":
            material_cost = manufacturing_job.get("material_cost")
            total_cost = manufacturing_job.get("total_cost")
            bpc_source = _get_blueprint_copy_source(source_row)
            bpo_source = _get_blueprint_original_source(source_row)
            meta_group = str(source_row.get("meta_group_name") or "")
            category = _get_product_category_name(source_row)

        flattened_rows.append(
            {
                "_path": current_path,
                "_parent_path": tree_path_separator.join(path_ids) if path_ids else "",
                "_depth": len(path_ids),
                "_sort_order": order_key,
                "_has_children": bool(node.get("children") or []),
                "ID": int(node.get("type_id") or source_row.get("type_id") or 0),
                "Step": str(node.get("label") or ""),
                "Type": _tree_node_type_label(node),
                "Activity": activity,
                "Qty": node.get("quantity"),
                "Runs": node.get("runs"),
                "Job Duration": (
                    format_duration(int(duration_seconds or 0)) if duration_seconds is not None else ""
                ),
                "Material Cost": material_cost,
                "Job Cost": job_cost,
                "Total Cost": total_cost,
                "BPC Source": bpc_source,
                "BPO Source": bpo_source,
                "Meta Group": meta_group,
                "Category": category,
                "Blueprint Source": str(node.get("blueprint_source_kind") or ""),
            }
        )

        children = node.get("children") or []
        if not isinstance(children, list):
            return
        for child_index, child in enumerate(children, start=1):
            if not isinstance(child, dict):
                continue
            child_label = str(child.get("label") or child.get("node_type") or f"child_{child_index}")
            child_key = f"{child_index:03d}:{child_label}"
            add_node(
                source_row=source_row,
                node=child,
                path_ids=[*path_ids, sibling_key],
                sibling_key=child_key,
                order_key=f"{order_key}.{child_index:03d}",
            )

    for row_index, overview_row in enumerate(overview_rows, start=1):
        manufacturing_job = _get_manufacturing_job(overview_row)
        job_tree = manufacturing_job.get("job_tree") or {}
        if not isinstance(job_tree, dict) or not job_tree:
            job_tree = {
                "label": _get_product_name(overview_row),
                "node_type": "product",
                "type_id": overview_row.get("type_id"),
                "quantity": _get_product_quantity(overview_row),
                "runs": _get_effective_runs(overview_row),
                "duration_seconds": manufacturing_job.get("time_seconds"),
                "total_job_cost": manufacturing_job.get("total_job_cost"),
                "children": [],
            }
        root_label = str(job_tree.get("label") or _get_product_name(overview_row) or f"product_{row_index}")
        root_key = f"{row_index:03d}:{root_label}"
        add_node(
            source_row=overview_row,
            node=job_tree,
            path_ids=[],
            sibling_key=root_key,
            order_key=f"{row_index:03d}",
        )

    return flattened_rows


def _visible_tree_rows(flattened_rows: list[dict[str, Any]], expanded_paths: set[str]) -> list[dict[str, Any]]:
    visible_rows: list[dict[str, Any]] = []
    for row in flattened_rows:
        if not isinstance(row, dict):
            continue
        parent_path = str(row.get("_parent_path") or "")
        if not parent_path:
            visible_rows.append(dict(row))
            continue

        ancestor_path = parent_path
        is_visible = True
        while ancestor_path:
            if ancestor_path not in expanded_paths:
                is_visible = False
                break
            if "|||" not in ancestor_path:
                ancestor_path = ""
            else:
                ancestor_path = ancestor_path.rsplit("|||", 1)[0]

        if is_visible:
            visible_rows.append(dict(row))
    return visible_rows


def _display_tree_step(row: dict[str, Any], expanded_paths: set[str]) -> str:
    depth = max(0, int(row.get("_depth") or 0))
    path = str(row.get("_path") or "")
    has_children = bool(row.get("_has_children", False))
    prefix = "  " * depth
    if has_children:
        marker = "▾ " if path in expanded_paths else "▸ "
    else:
        marker = "• "
    return f"{prefix}{marker}{str(row.get('Step') or '')}"


def _build_debug_payload_preview(row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    manufacturing_job = row.get("manufacturing_job") or {}
    if not isinstance(manufacturing_job, dict):
        manufacturing_job = {}
    return {
        "overview_row_id": row.get("overview_row_id"),
        "type_id": row.get("type_id"),
        "type_name": row.get("type_name"),
        "quantity": row.get("quantity"),
        "meta_group_name": row.get("meta_group_name"),
        "manufacturing_job": {
            "runs": manufacturing_job.get("runs"),
            "time_seconds": manufacturing_job.get("time_seconds"),
            "manufacturing_time_seconds": manufacturing_job.get("manufacturing_time_seconds"),
            "material_cost": manufacturing_job.get("material_cost"),
            "total_job_cost": manufacturing_job.get("total_job_cost"),
            "total_cost": manufacturing_job.get("total_cost"),
            "blueprint_source_kind": manufacturing_job.get("blueprint_source_kind"),
            "blueprint_material_efficiency": manufacturing_job.get("blueprint_material_efficiency"),
            "blueprint_time_efficiency": manufacturing_job.get("blueprint_time_efficiency"),
            "activity_breakdown": manufacturing_job.get("activity_breakdown"),
            "recursive_activity_breakdown": manufacturing_job.get("recursive_activity_breakdown"),
            "materials_count": len((manufacturing_job.get("materials") or {})),
            "procurement_materials_count": len((manufacturing_job.get("procurement_materials") or {})),
            "job_tree_child_count": len(((manufacturing_job.get("job_tree") or {}).get("children") or [])),
        },
    }


def render() -> None:
    st.subheader("Industry Builder")

    try:
        characters = fetch_characters()
    except Exception as e:
        st.error(f"Failed to load characters: {e}")
        return

    if not characters:
        st.warning("No character data found. Run main.py first.")
        return

    character_options = build_character_options(characters)
    if not character_options:
        st.warning("No character data found. Run main.py first.")
        return

    character_ids = list(character_options.keys())
    default_character_id = character_ids[0]
    for character in characters:
        if not isinstance(character, dict):
            continue
        if not bool(character.get("is_main")):
            continue
        try:
            main_character_id = int(character.get("character_id") or 0)
        except Exception:
            continue
        if main_character_id in character_options:
            default_character_id = main_character_id
            break

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

    if "industry_builder_owned_blueprints_scope" not in st.session_state:
        st.session_state["industry_builder_owned_blueprints_scope"] = default_owned_blueprint_scope
    elif str(st.session_state.get("industry_builder_owned_blueprints_scope", "")) not in owned_blueprint_scope_options:
        st.session_state["industry_builder_owned_blueprints_scope"] = default_owned_blueprint_scope
    if "industry_builder_owned_blueprints_scope_applied" not in st.session_state:
        st.session_state["industry_builder_owned_blueprints_scope_applied"] = default_owned_blueprint_scope
    if "industry_builder_character_id" not in st.session_state:
        st.session_state["industry_builder_character_id"] = int(default_character_id)
    elif int(st.session_state.get("industry_builder_character_id", default_character_id)) not in character_options:
        st.session_state["industry_builder_character_id"] = int(default_character_id)
    if "industry_builder_character_id_applied" not in st.session_state:
        st.session_state["industry_builder_character_id_applied"] = int(default_character_id)
    if "industry_builder_industry_profile_id" not in st.session_state:
        st.session_state["industry_builder_industry_profile_id"] = 0
    if "industry_builder_industry_profile_id_applied" not in st.session_state:
        st.session_state["industry_builder_industry_profile_id_applied"] = 0

    selector_col_left, selector_col_mid, selector_col_right = st.columns(3)
    with selector_col_left:
        owned_blueprints_scope = st.selectbox(
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

    try:
        industry_profiles = fetch_industry_profiles(character_id=int(selected_character_id))
    except Exception as e:
        st.error(f"Failed to load industry profiles: {e}")
        return

    industry_profile_options, industry_profile_labels, default_industry_profile_id = build_industry_profile_options(
        cast(list[dict[str, Any]], industry_profiles)
    )
    if int(st.session_state.get("industry_builder_industry_profile_id", 0)) not in industry_profile_options:
        st.session_state["industry_builder_industry_profile_id"] = int(default_industry_profile_id)

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

    if "industry_builder_maximize_bp_runs_pending" not in st.session_state:
        st.session_state["industry_builder_maximize_bp_runs_pending"] = True
    if "industry_builder_maximize_bp_runs_applied" not in st.session_state:
        st.session_state["industry_builder_maximize_bp_runs_applied"] = True
    if "industry_builder_build_from_bpc" not in st.session_state:
        st.session_state["industry_builder_build_from_bpc"] = True
    if "industry_builder_build_from_bpc_applied" not in st.session_state:
        st.session_state["industry_builder_build_from_bpc_applied"] = True
    if "industry_builder_have_blueprint_source_only" not in st.session_state:
        st.session_state["industry_builder_have_blueprint_source_only"] = True
    if "industry_builder_have_blueprint_source_only_applied" not in st.session_state:
        st.session_state["industry_builder_have_blueprint_source_only_applied"] = True
    if "industry_builder_include_reactions" not in st.session_state:
        st.session_state["industry_builder_include_reactions"] = False
    if "industry_builder_include_reactions_applied" not in st.session_state:
        st.session_state["industry_builder_include_reactions_applied"] = False
    if "industry_builder_have_skills_only" not in st.session_state:
        st.session_state["industry_builder_have_skills_only"] = True

    solar_system_security_map: dict[int, float] = {}
    try:
        solar_system_security_map = _fetch_solar_system_security_map()
    except Exception as e:
        st.warning(f"Failed to load solar system security status: {e}")

    selected_profile_system_id: int | None = None
    for profile in industry_profiles:
        if int(profile.get("id") or 0) != int(selected_industry_profile_id):
            continue
        try:
            selected_profile_system_id = int(profile.get("system_id") or 0) or None
        except Exception:
            selected_profile_system_id = None
        break

    selected_profile_security_status = (
        float(solar_system_security_map.get(selected_profile_system_id or 0, 0.0)) if selected_profile_system_id else None
    )
    reactions_allowed_for_profile = (
        selected_profile_security_status is None or selected_profile_security_status < 0.5
    )
    if not reactions_allowed_for_profile:
        st.session_state["industry_builder_include_reactions"] = False

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

    st.caption(
        "Manufacturable product overview derived from the SDE blueprints and enriched with type metadata. "
        "Each product row contains a simplified manufacturing job payload with materials, skills, time, and production limits."
    )

    if "industry_builder_overview_rows" not in st.session_state:
        try:
            st.session_state["industry_builder_overview_rows"] = _fetch_product_overview(
                force_refresh=False,
                maximize_bp_runs=bool(st.session_state.get("industry_builder_maximize_bp_runs_applied", False)),
                build_from_bpc=bool(st.session_state.get("industry_builder_build_from_bpc_applied", True)),
                have_blueprint_source_only=bool(
                    st.session_state.get("industry_builder_have_blueprint_source_only_applied", True)
                ),
                include_reactions=bool(st.session_state.get("industry_builder_include_reactions_applied", False)),
                industry_profile_id=int(st.session_state.get("industry_builder_industry_profile_id_applied", 0)) or None,
                owned_blueprints_scope=str(st.session_state.get("industry_builder_owned_blueprints_scope_applied", default_owned_blueprint_scope)),
                character_id=int(st.session_state.get("industry_builder_character_id_applied", default_character_id)),
            )
        except Exception as e:
            st.error(f"Failed to load industry product overview: {e}")
            return

    if "industry_builder_job_manager_status" not in st.session_state:
        try:
            st.session_state["industry_builder_job_manager_status"] = _fetch_job_manager_status()
        except Exception as e:
            st.warning(f"Failed to load industry job manager status: {e}")
            st.session_state["industry_builder_job_manager_status"] = {}

    job_manager_status = cast(dict[str, Any], st.session_state.get("industry_builder_job_manager_status") or {})

    if job_manager_status:
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

    overview_rows = cast(list[dict[str, Any]], st.session_state.get("industry_builder_overview_rows") or [])

    if not overview_rows:
        st.info("No manufacturable product rows are available yet.")
        return

    meta_group_names = _ordered_meta_group_names({_get_meta_group_name(row) for row in overview_rows})

    if meta_group_names:
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
            {"No Meta Group"},
        ]
        enabled_meta_groups: set[str] = set()
        for meta_group_name in meta_group_names:
            toggle_key = _meta_group_toggle_key(meta_group_name)
            label = _meta_group_label(meta_group_name)
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
                    value=bool(st.session_state.get(toggle_key, False)),
                    key=toggle_key,
                )
            if enabled:
                enabled_meta_groups.add(meta_group_name)
    else:
        enabled_meta_groups = set()

    refresh_col_left, refresh_col_right = st.columns([6, 1])
    with refresh_col_left:
        st.caption("Backend-backed changes are applied only after clicking Refresh Overview.")
    with refresh_col_right:
        if st.button("Refresh Overview", key="industry_builder_refresh_overview"):
            st.session_state["industry_builder_maximize_bp_runs_applied"] = bool(
                st.session_state.get("industry_builder_maximize_bp_runs_pending", False)
            )
            st.session_state["industry_builder_build_from_bpc_applied"] = bool(
                st.session_state.get("industry_builder_build_from_bpc", True)
            )
            st.session_state["industry_builder_have_blueprint_source_only_applied"] = bool(
                st.session_state.get("industry_builder_have_blueprint_source_only", True)
            )
            st.session_state["industry_builder_include_reactions_applied"] = (
                bool(st.session_state.get("industry_builder_include_reactions", False)) and reactions_allowed_for_profile
            )
            st.session_state["industry_builder_owned_blueprints_scope_applied"] = str(
                st.session_state.get("industry_builder_owned_blueprints_scope", default_owned_blueprint_scope)
            )
            st.session_state["industry_builder_character_id_applied"] = int(
                st.session_state.get("industry_builder_character_id", default_character_id)
            )
            st.session_state["industry_builder_industry_profile_id_applied"] = int(
                st.session_state.get("industry_builder_industry_profile_id", default_industry_profile_id)
            )
            _fetch_product_overview.clear()
            _fetch_job_manager_status.clear()
            progress_placeholder = st.empty()
            progress_bar = progress_placeholder.progress(0, text="Starting overview refresh...")
            try:
                refresh_job = _start_product_overview_refresh(
                    maximize_bp_runs=bool(st.session_state.get("industry_builder_maximize_bp_runs_applied", False)),
                    build_from_bpc=bool(st.session_state.get("industry_builder_build_from_bpc_applied", True)),
                    have_blueprint_source_only=bool(
                        st.session_state.get("industry_builder_have_blueprint_source_only_applied", True)
                    ),
                    include_reactions=bool(st.session_state.get("industry_builder_include_reactions_applied", False)),
                    industry_profile_id=int(st.session_state.get("industry_builder_industry_profile_id_applied", 0)) or None,
                    owned_blueprints_scope=str(
                        st.session_state.get("industry_builder_owned_blueprints_scope_applied", default_owned_blueprint_scope)
                    ),
                    character_id=int(st.session_state.get("industry_builder_character_id_applied", default_character_id)),
                )
                refresh_job_id = str(refresh_job.get("job_id") or "")
                if not refresh_job_id:
                    raise RuntimeError("Refresh job did not return a job_id")

                while True:
                    refresh_status = _fetch_product_overview_refresh_status(refresh_job_id)
                    progress_fraction = float(refresh_status.get("progress_fraction") or 0.0)
                    progress_label = str(refresh_status.get("progress_label") or "Refreshing overview...")
                    progress_bar.progress(int(max(0.0, min(1.0, progress_fraction)) * 100), text=progress_label)
                    status = str(refresh_status.get("status") or "")
                    if status == "completed":
                        st.session_state["industry_builder_overview_rows"] = cast(
                            list[dict[str, Any]], refresh_status.get("result") or []
                        )
                        st.session_state["industry_builder_job_manager_status"] = _fetch_job_manager_status()
                        break
                    if status == "failed":
                        raise RuntimeError(refresh_status.get("error_message") or "Refresh job failed")
                    time.sleep(1.0)
            except Exception as e:
                st.error(f"Failed to refresh industry product overview: {e}")
                return
            finally:
                progress_placeholder.empty()
            _rerun()

    filtered_overview_rows = [
        row
        for row in overview_rows
        if _get_meta_group_name(row) in enabled_meta_groups
        and (
            not bool(st.session_state.get("industry_builder_have_skills_only", True))
            or _get_skill_requirements_met(row)
        )
    ]

    if not filtered_overview_rows:
        st.info("No manufacturable product rows match the current meta group filters.")
        return

    tree_rows = _flatten_overview_job_tree_rows(filtered_overview_rows)
    valid_paths = {str(row.get("_path") or "") for row in tree_rows if isinstance(row, dict)}
    if "industry_builder_expanded_tree_paths" not in st.session_state:
        st.session_state["industry_builder_expanded_tree_paths"] = set()
    expanded_tree_paths = {
        str(path)
        for path in cast(set[str], st.session_state.get("industry_builder_expanded_tree_paths") or set())
        if str(path) in valid_paths
    }
    st.session_state["industry_builder_expanded_tree_paths"] = expanded_tree_paths

    visible_tree_rows = _visible_tree_rows(tree_rows, expanded_tree_paths)
    for row in visible_tree_rows:
        row["Step"] = _display_tree_step(row, expanded_tree_paths)

    df = pd.DataFrame(visible_tree_rows).sort_values(by=["_sort_order"], ascending=[True]).reset_index(drop=True)
    st.caption("Click a row with a ▸ or ▾ marker in Step to expand or collapse that branch.")

    gb = grid_options_builder.from_dataframe(df)
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
        ("Step", 320),
        ("Type", 110),
        ("Activity", 120),
        ("Blueprint Source", 150),
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

    for col in [
        "ID",
        "Qty",
        "Runs",
    ]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=js_code, locale=eu_locale, decimals=0),
                minWidth=105,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    for col in ["_path", "_parent_path", "_depth", "_sort_order", "_has_children"]:
        if col in df.columns:
            gb.configure_column(col, hide=True)

    for col in [
        "Material Cost",
        "Job Cost",
        "Total Cost",
    ]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=js_code, locale=eu_locale, decimals=2),
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
    height = min(1100, 120 + (len(visible_tree_rows) * 34))
    grid_state_key = abs(hash("|".join(sorted(expanded_tree_paths))))
    grid_response = aggrid_fn(
        df,
        gridOptions=grid_options,
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=height,
        fit_columns_on_grid_load=True,
        key=f"industry_builder_products_overview_{grid_state_key}",
    )

    selected_rows = []
    if isinstance(grid_response, dict):
        raw_selected_rows = grid_response.get("selected_rows") or []
        if isinstance(raw_selected_rows, list):
            selected_rows = raw_selected_rows
        elif hasattr(raw_selected_rows, "to_dict"):
            try:
                selected_rows = cast(list[dict[str, Any]], raw_selected_rows.to_dict("records"))
            except Exception:
                selected_rows = []

    selected_path = ""
    selected_has_children = False
    if selected_rows:
        selected_row = cast(dict[str, Any], selected_rows[0] or {})
        selected_path = str(selected_row.get("_path") or "")
        selected_has_children = bool(selected_row.get("_has_children", False))

    last_selected_path = str(st.session_state.get("industry_builder_last_selected_tree_path") or "")
    if selected_path and selected_has_children and selected_path != last_selected_path:
        if selected_path in expanded_tree_paths:
            expanded_tree_paths.remove(selected_path)
        else:
            expanded_tree_paths.add(selected_path)
        st.session_state["industry_builder_expanded_tree_paths"] = expanded_tree_paths
        st.session_state["industry_builder_last_selected_tree_path"] = selected_path
        _rerun()
    elif not selected_path and last_selected_path:
        st.session_state["industry_builder_last_selected_tree_path"] = ""

    debug_options: dict[str, str] = {}
    for row in filtered_overview_rows:
        overview_row_id = str(row.get("overview_row_id") or "")
        product_name = str(row.get("type_name") or row.get("type_id") or "")
        product_type_id = int(row.get("type_id") or 0)
        label = f"{product_name} ({product_type_id})"
        debug_options[overview_row_id] = label

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
            st.write(_build_debug_payload_preview(selected_debug_payload))
            if st.checkbox("Show full nested payload", key="industry_builder_show_full_debug_payload"):
                st.write(selected_debug_payload or {})
    else:
        with st.expander("Raw data (for debugging)", expanded=False):
            st.write({})
