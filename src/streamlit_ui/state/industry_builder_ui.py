from __future__ import annotations

import hashlib
from typing import Any, cast

import pandas as pd
import streamlit as st

from streamlit_ui.components.assets_data import get_item_image_url
from streamlit_ui.components.formatters import format_duration


def get_manufacturing_job(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("manufacturing_job") or {}
    return value if isinstance(value, dict) else {}


def get_node_blueprint_sources(node: dict[str, Any]) -> tuple[str, str]:
    blueprint_copy = node.get("blueprint_copy") or {}
    blueprint_original = node.get("blueprint_original") or {}
    bpc_source = _format_blueprint_owner_source(blueprint_copy) if isinstance(blueprint_copy, dict) else ""
    bpo_source = _format_blueprint_owner_source(blueprint_original) if isinstance(blueprint_original, dict) else ""
    return bpc_source, bpo_source


def get_node_primary_activity_child(node: dict[str, Any]) -> dict[str, Any]:
    children = node.get("children") or []
    if not isinstance(children, list):
        return {}
    for child in children:
        if not isinstance(child, dict):
            continue
        child_node_type = str(child.get("node_type") or "").strip().lower()
        child_activity = str(child.get("activity") or "").strip().lower()
        if child_node_type == "activity" and child_activity in {"manufacturing", "reaction"}:
            return child
    return {}


def get_product_quantity(row: dict[str, Any]) -> int:
    try:
        return int(row.get("quantity") or 0)
    except Exception:
        return 0


def get_effective_runs(row: dict[str, Any]) -> int:
    manufacturing_job = get_manufacturing_job(row)
    try:
        runs = int(manufacturing_job.get("runs") or 0)
    except Exception:
        runs = 0
    if runs > 0:
        return runs
    return get_product_quantity(row)


def skill_requirements_met(row: dict[str, Any]) -> bool:
    skills = get_manufacturing_job(row).get("skills") or {}
    if not isinstance(skills, dict):
        return False
    return bool(skills.get("skill_requirements_met", False))


def get_meta_group_name(row: dict[str, Any]) -> str:
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


def meta_group_label(meta_group_name: str) -> str:
    return meta_group_name if meta_group_name else "Other"


def meta_group_toggle_key(meta_group_name: str) -> str:
    normalized = meta_group_name.strip().lower() or "none"
    safe = "".join(ch if ch.isalnum() else "_" for ch in normalized)
    return f"industry_builder_meta_group_{safe}"


def ordered_meta_group_names(meta_group_names: set[str]) -> list[str]:
    preferred_order = [
        "Tech I",
        "Tech II",
        "Tech III",
        "Faction",
        "Storyline",
        "Officer",
        "Other",
    ]
    ordered: list[str] = []
    available = {meta_group_label(name): name for name in meta_group_names}
    for label in preferred_order:
        if label in available:
            ordered.append(available[label])

    remaining = sorted(
        [name for name in meta_group_names if meta_group_label(name) not in preferred_order],
        key=lambda name: meta_group_label(name).lower(),
    )
    ordered.extend(remaining)
    return ordered


def blueprint_step_name(source_row: dict[str, Any], node: dict[str, Any]) -> str:
    blueprint_name = str(node.get("blueprint_name") or "").strip()
    if blueprint_name:
        return blueprint_name

    manufacturing_job = cast(dict[str, Any], source_row.get("manufacturing_job") or {})
    blueprint_sde = manufacturing_job.get("blueprint_sde") or {}
    if isinstance(blueprint_sde, dict):
        blueprint_name = str(blueprint_sde.get("type_name") or blueprint_sde.get("name") or "").strip()
        if blueprint_name:
            return blueprint_name

    return "Blueprint"


def tree_node_step_label(source_row: dict[str, Any], node: dict[str, Any]) -> str:
    activity = str(node.get("activity") or "").strip().lower()
    label = str(node.get("label") or "")
    if activity == "invention":
        return f"{blueprint_step_name(source_row, node)} Copy (invention)"
    if activity == "copying":
        return f"{blueprint_step_name(source_row, node)} Copy"
    return label


def tree_node_type_label(node: dict[str, Any]) -> str:
    node_type = str(node.get("node_type") or "").strip().lower()
    activity = str(node.get("activity") or "").strip().lower()
    if node_type == "product" or activity == "manufacturing":
        return "Manufacture"
    if activity == "reaction":
        return "Reaction"
    if activity == "invention":
        return "Invention"
    if activity == "copying":
        return "Copying"
    if activity in {"research_material", "research_time"}:
        return "Research"
    return ""


def tree_node_activity_label(node: dict[str, Any]) -> str:
    node_type = str(node.get("node_type") or "").strip().lower()
    activity = str(node.get("activity") or "").strip().lower()
    recommendation_action = str(node.get("recommendation_action") or "").strip().lower()
    sourcing_strategy = str(node.get("sourcing_strategy") or "").strip().lower()
    runs = int(node.get("runs") or 0)

    if recommendation_action in {"build", "take", "buy", "copy", "invent", "research"}:
        return recommendation_action

    if node_type == "product" or activity in {"manufacturing", "reaction"}:
        return "build"
    if activity == "invention":
        return "invent" if runs > 0 else "take"
    if activity == "copying":
        return "copy" if runs > 0 else "take"
    if activity in {"research_material", "research_time"}:
        return "research"
    if node_type == "material":
        if sourcing_strategy == "build":
            return "build"
        if sourcing_strategy == "take":
            return "take"
        if sourcing_strategy in {"buy", "buy_reaction_available"}:
            return "buy"
        return "take or buy"
    return ""


def tree_node_icon_url(source_row: dict[str, Any], node: dict[str, Any]) -> str:
    node_type = str(node.get("node_type") or "").strip().lower()
    activity = str(node.get("activity") or "").strip().lower()

    try:
        type_id = int(node.get("type_id") or source_row.get("type_id") or 0)
    except Exception:
        type_id = 0

    try:
        blueprint_type_id = int(
            node.get("blueprint_type_id")
            or ((source_row.get("manufacturing_job") or {}).get("blueprint_sde") or {}).get("blueprint_type_id")
            or source_row.get("blueprint_type_id")
            or 0
        )
    except Exception:
        blueprint_type_id = 0

    if node_type in {"product", "material"} and type_id > 0:
        category_name = str(node.get("category_name") or source_row.get("category_name") or "")
        return get_item_image_url(type_id=type_id, type_category_name=category_name, is_blueprint_copy=False, size=32)

    if activity in {"manufacturing", "reaction"} and type_id > 0:
        category_name = str(node.get("category_name") or source_row.get("category_name") or "")
        return get_item_image_url(type_id=type_id, type_category_name=category_name, is_blueprint_copy=False, size=32)

    if activity in {"copying", "invention"} and blueprint_type_id > 0:
        return get_item_image_url(type_id=blueprint_type_id, type_category_name="Blueprint", is_blueprint_copy=True, size=32)

    if activity in {"research_material", "research_time"} and blueprint_type_id > 0:
        return get_item_image_url(type_id=blueprint_type_id, type_category_name="Blueprint", is_blueprint_copy=False, size=32)

    return ""


@st.cache_data(ttl=300, show_spinner=False)
def flatten_overview_job_tree_rows(overview_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        manufacturing_job = get_manufacturing_job(source_row)
        node_type = str(node.get("node_type") or "")
        activity = str(node.get("activity") or "")
        duration_seconds = node.get("duration_seconds")
        job_cost = node.get("total_job_cost")
        if job_cost is None:
            job_cost = node.get("job_cost")

        material_cost = node.get("material_cost")
        total_cost = node.get("total_cost")
        if total_cost is None:
            total_cost = job_cost
        market_unit_price = node.get("unit_price")
        market_price_source = node.get("price_source")
        market_volume = node.get("price_volume_total") or node.get("volume_total")
        market_hub = node.get("market_hub_label") or node.get("market_hub")
        region_daily_volume = node.get("region_daily_volume")
        region_daily_volume_7d_avg = node.get("region_daily_volume_7d_avg")
        hub_buy_liquidity = node.get("hub_buy_liquidity")
        hub_sell_liquidity = node.get("hub_sell_liquidity")
        hub_buy_order_count = node.get("hub_buy_order_count")
        hub_sell_order_count = node.get("hub_sell_order_count")
        gross_sale_value = node.get("gross_sale_value")
        broker_fee_amount = node.get("broker_fee_amount")
        sales_tax_amount = node.get("sales_tax_amount")
        net_proceeds = node.get("net_proceeds")
        profit_amount = node.get("profit_amount")
        profit_margin_fraction = node.get("profit_margin_fraction")
        isk_per_hour = node.get("isk_per_hour")
        pricing_confidence = node.get("pricing_confidence")
        days_of_supply = node.get("days_of_supply")
        sell_through_rate = node.get("sell_through_rate")
        liquidity_indicator = node.get("liquidity_indicator")
        liquidity_score = node.get("liquidity_score")
        price_anomaly_risk = node.get("price_anomaly_risk")
        price_anomaly_reasons = node.get("price_anomaly_reasons")
        price_vs_material_ratio = node.get("price_vs_material_ratio")
        price_vs_history_ratio = node.get("price_vs_history_ratio")
        history_7d_avg = node.get("history_7d_avg")
        return_on_capital = node.get("return_on_capital")
        manufacture_window_ok = node.get("manufacture_window_ok")
        blueprint_me = node.get("blueprint_me")
        prep_time_fraction_pct = node.get("prep_time_fraction_pct")
        fragile_margin = node.get("fragile_margin")
        blueprint_sde_fallback = node.get("blueprint_sde_fallback")
        material_contention = node.get("material_contention")
        manufacturing_cost_index = node.get("manufacturing_cost_index")
        bpc_source = ""
        bpo_source = ""
        meta_group = str(node.get("meta_group_name") or "")
        category = str(node.get("category_name") or "")
        current_path = tree_path_separator.join([*path_ids, sibling_key])

        node_bpc_source, node_bpo_source = get_node_blueprint_sources(node)
        if node_bpc_source:
            bpc_source = node_bpc_source
        if node_bpo_source:
            bpo_source = node_bpo_source

        primary_activity_child = get_node_primary_activity_child(node)
        if primary_activity_child:
            if node.get("runs") in {None, ""}:
                inherited_runs = primary_activity_child.get("runs")
                if inherited_runs not in {None, ""}:
                    node["runs"] = inherited_runs
            if duration_seconds in {None, ""}:
                inherited_duration_seconds = primary_activity_child.get("duration_seconds")
                if inherited_duration_seconds not in {None, ""}:
                    duration_seconds = inherited_duration_seconds

            child_bpc_source, child_bpo_source = get_node_blueprint_sources(primary_activity_child)
            if child_bpc_source and not bpc_source:
                bpc_source = child_bpc_source
            if child_bpo_source and not bpo_source:
                bpo_source = child_bpo_source

        if node_type == "product" and int(node.get("type_id") or 0) == int(source_row.get("type_id") or 0):
            material_cost = manufacturing_job.get("material_cost")
            job_cost = manufacturing_job.get("total_job_cost")
            total_cost = manufacturing_job.get("total_cost")
            market_unit_price = source_row.get("market_unit_price")
            market_price_source = source_row.get("market_price_source")
            market_volume = source_row.get("market_volume_total")
            market_hub = source_row.get("market_hub_label") or source_row.get("market_hub")
            region_daily_volume = source_row.get("region_daily_volume")
            region_daily_volume_7d_avg = source_row.get("region_daily_volume_7d_avg")
            hub_buy_liquidity = source_row.get("hub_buy_liquidity")
            hub_sell_liquidity = source_row.get("hub_sell_liquidity")
            hub_buy_order_count = source_row.get("hub_buy_order_count")
            hub_sell_order_count = source_row.get("hub_sell_order_count")
            gross_sale_value = source_row.get("gross_sale_value")
            broker_fee_amount = source_row.get("broker_fee_amount")
            sales_tax_amount = source_row.get("sales_tax_amount")
            net_proceeds = source_row.get("net_proceeds")
            profit_amount = source_row.get("profit_amount")
            profit_margin_fraction = source_row.get("profit_margin_fraction")
            isk_per_hour = source_row.get("isk_per_hour")
            pricing_confidence = source_row.get("pricing_confidence")
            days_of_supply = source_row.get("days_of_supply")
            sell_through_rate = source_row.get("sell_through_rate")
            liquidity_indicator = source_row.get("liquidity_indicator")
            liquidity_score = source_row.get("liquidity_score")
            price_anomaly_risk = source_row.get("price_anomaly_risk")
            price_anomaly_reasons = source_row.get("price_anomaly_reasons")
            price_vs_material_ratio = source_row.get("price_vs_material_ratio")
            price_vs_history_ratio = source_row.get("price_vs_history_ratio")
            history_7d_avg = source_row.get("history_7d_avg")
            return_on_capital = source_row.get("return_on_capital")
            manufacture_window_ok = source_row.get("manufacture_window_ok")
            blueprint_me = source_row.get("blueprint_me")
            prep_time_fraction_pct = source_row.get("prep_time_fraction_pct")
            fragile_margin = source_row.get("fragile_margin")
            blueprint_sde_fallback = source_row.get("blueprint_sde_fallback")
            material_contention = source_row.get("material_contention")
            manufacturing_cost_index = source_row.get("manufacturing_cost_index")
            blueprint_copy = manufacturing_job.get("blueprint_copy") or {}
            blueprint_original = manufacturing_job.get("blueprint_original") or {}
            if isinstance(blueprint_copy, dict):
                bpc_source = _format_blueprint_owner_source(blueprint_copy)
            if isinstance(blueprint_original, dict):
                bpo_source = _format_blueprint_owner_source(blueprint_original)
            meta_group = str(node.get("meta_group_name") or source_row.get("meta_group_name") or "")
            category = str(node.get("category_name") or source_row.get("category_name") or "")
        elif activity == "manufacturing":
            if int(node.get("type_id") or 0) == int(source_row.get("type_id") or 0):
                material_cost = manufacturing_job.get("material_cost")
                total_cost = manufacturing_job.get("total_cost")
                market_unit_price = source_row.get("market_unit_price")
                market_price_source = source_row.get("market_price_source")
                market_volume = source_row.get("market_volume_total")
                market_hub = source_row.get("market_hub_label") or source_row.get("market_hub")
                region_daily_volume = source_row.get("region_daily_volume")
                region_daily_volume_7d_avg = source_row.get("region_daily_volume_7d_avg")
                hub_buy_liquidity = source_row.get("hub_buy_liquidity")
                hub_sell_liquidity = source_row.get("hub_sell_liquidity")
                hub_buy_order_count = source_row.get("hub_buy_order_count")
                hub_sell_order_count = source_row.get("hub_sell_order_count")
                gross_sale_value = source_row.get("gross_sale_value")
                broker_fee_amount = source_row.get("broker_fee_amount")
                sales_tax_amount = source_row.get("sales_tax_amount")
                net_proceeds = source_row.get("net_proceeds")
                profit_amount = source_row.get("profit_amount")
                profit_margin_fraction = source_row.get("profit_margin_fraction")
                isk_per_hour = source_row.get("isk_per_hour")
                pricing_confidence = source_row.get("pricing_confidence")
                days_of_supply = source_row.get("days_of_supply")
                sell_through_rate = source_row.get("sell_through_rate")
                liquidity_indicator = source_row.get("liquidity_indicator")
                liquidity_score = source_row.get("liquidity_score")
                blueprint_copy = manufacturing_job.get("blueprint_copy") or {}
                blueprint_original = manufacturing_job.get("blueprint_original") or {}
                if isinstance(blueprint_copy, dict):
                    bpc_source = _format_blueprint_owner_source(blueprint_copy)
                if isinstance(blueprint_original, dict):
                    bpo_source = _format_blueprint_owner_source(blueprint_original)
            meta_group = str(node.get("meta_group_name") or source_row.get("meta_group_name") or "")
            category = str(node.get("category_name") or source_row.get("category_name") or "")

        explicit_item_id = int(node.get("item_id") or 0)
        type_id_value = int(node.get("type_id") or source_row.get("type_id") or 0)
        display_id = explicit_item_id if explicit_item_id > 0 else (
            type_id_value if str(node_type).strip().lower() in {"product", "material"} and type_id_value > 0 else None
        )

        flattened_rows.append(
            {
                "_path": current_path,
                "_parent_path": tree_path_separator.join(path_ids) if path_ids else "",
                "_depth": len(path_ids),
                "_sort_order": order_key,
                "_has_children": bool(node.get("children") or []),
                "ID": display_id,
                "Step": tree_node_step_label(source_row, node),
                "Icon": tree_node_icon_url(source_row, node),
                "Activity": tree_node_activity_label(node),
                "Qty": node.get("quantity"),
                "Runs": node.get("runs"),
                "Job Duration": (
                    format_duration(int(duration_seconds or 0))
                    if duration_seconds is not None and int(duration_seconds or 0) > 0
                    else ""
                ),
                "Profit": profit_amount,
                "Profit Margin %": (float(profit_margin_fraction) * 100.0 if profit_margin_fraction is not None else None),
                "ISK/Hour": isk_per_hour,
                "Net Proceeds": net_proceeds,
                "Total Cost": total_cost,
                "Material Cost": material_cost,
                "Job Cost": job_cost,
                "Broker Fee": broker_fee_amount,
                "Sales Tax": sales_tax_amount,
                "Market Unit Price": market_unit_price,
                "Gross Sale Value": gross_sale_value,
                "Type": tree_node_type_label(node),
                "Pricing Confidence": pricing_confidence,
                "Market Price Source": market_price_source,
                "Market Volume": market_volume,
                "Region Daily Volume": region_daily_volume,
                "Region Daily Volume (7d Avg)": region_daily_volume_7d_avg,
                "Hub Buy Liquidity": hub_buy_liquidity,
                "Hub Sell Liquidity": hub_sell_liquidity,
                "Hub Buy Orders": hub_buy_order_count,
                "Hub Sell Orders": hub_sell_order_count,
                "Days of Supply": days_of_supply,
                "Sell-Through Rate %": sell_through_rate,
                "Liquidity Indicator": liquidity_indicator,
                "Liquidity Score": liquidity_score,
                "Price Anomaly Risk": price_anomaly_risk,
                "Price Anomaly Reasons": (
                    "; ".join(price_anomaly_reasons) if isinstance(price_anomaly_reasons, list) and price_anomaly_reasons else None
                ),
                "Price vs Material Ratio": price_vs_material_ratio,
                "Price vs History Ratio": price_vs_history_ratio,
                "History 7d Avg": history_7d_avg,
                "Return on Capital %": (float(return_on_capital) * 100.0 if return_on_capital is not None else None),
                "Manufacture Window": (
                    "OK" if manufacture_window_ok is True
                    else "⚠ At Risk" if manufacture_window_ok is False
                    else None
                ),
                "Blueprint ME": blueprint_me,
                "Prep Time %": prep_time_fraction_pct,
                "Fragile Margin": ("⚠ Yes" if fragile_margin else None),
                "SDE Fallback": ("⚠ No Blueprint" if blueprint_sde_fallback else None),
                "Material Contention": ("⚠ Yes" if material_contention else None),
                "Mfg Cost Index %": (float(manufacturing_cost_index) * 100.0 if manufacturing_cost_index is not None else None),
                "Market Hub": market_hub,
                "BPC Source": bpc_source,
                "BPO Source": bpo_source,
                "Meta Group": meta_group,
                "Category": category,
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
        manufacturing_job = cast(dict[str, Any], overview_row.get("manufacturing_job") or {})
        job_tree = manufacturing_job.get("job_tree") or {}
        if not isinstance(job_tree, dict) or not job_tree:
            job_tree = {
                "label": str(overview_row.get("type_name") or ""),
                "node_type": "product",
                "type_id": overview_row.get("type_id"),
                "quantity": get_product_quantity(overview_row),
                "runs": get_effective_runs(overview_row),
                "duration_seconds": manufacturing_job.get("time_seconds"),
                "total_job_cost": manufacturing_job.get("total_job_cost"),
                "children": [],
            }
        root_label = str(job_tree.get("label") or overview_row.get("type_name") or f"product_{row_index}")
        root_key = f"{row_index:03d}:{root_label}"
        add_node(
            source_row=overview_row,
            node=job_tree,
            path_ids=[],
            sibling_key=root_key,
            order_key=f"{row_index:03d}",
        )

    return flattened_rows


def build_debug_payload_preview(row: dict[str, Any] | None) -> dict[str, Any]:
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
            "gross_sale_value": manufacturing_job.get("gross_sale_value"),
            "broker_fee_amount": manufacturing_job.get("broker_fee_amount"),
            "sales_tax_amount": manufacturing_job.get("sales_tax_amount"),
            "net_proceeds": manufacturing_job.get("net_proceeds"),
            "region_daily_volume": manufacturing_job.get("region_daily_volume"),
            "region_daily_volume_7d_avg": manufacturing_job.get("region_daily_volume_7d_avg"),
            "region_daily_volume_7d_sample_size": manufacturing_job.get("region_daily_volume_7d_sample_size"),
            "region_daily_order_count": manufacturing_job.get("region_daily_order_count"),
            "region_daily_volume_date": manufacturing_job.get("region_daily_volume_date"),
            "hub_buy_liquidity": manufacturing_job.get("hub_buy_liquidity"),
            "hub_sell_liquidity": manufacturing_job.get("hub_sell_liquidity"),
            "hub_buy_order_count": manufacturing_job.get("hub_buy_order_count"),
            "hub_sell_order_count": manufacturing_job.get("hub_sell_order_count"),
            "pricing_confidence": manufacturing_job.get("pricing_confidence"),
            "pricing_confidence_reasons": manufacturing_job.get("pricing_confidence_reasons"),
            "market_price_age_minutes": manufacturing_job.get("market_price_age_minutes"),
            "profit_amount": manufacturing_job.get("profit_amount"),
            "profit_margin_fraction": manufacturing_job.get("profit_margin_fraction"),
            "isk_per_hour": manufacturing_job.get("isk_per_hour"),
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


@st.cache_data(ttl=300, show_spinner=False)
def filter_overview_rows(
    overview_rows: list[dict[str, Any]],
    enabled_meta_groups: tuple[str, ...],
    have_skills_only: bool,
    positive_profit_only: bool,
    min_margin_pct: float,
    min_isk_per_hour: float,
    min_region_daily_volume: int,
    excluded_liquidity_indicators: tuple[str, ...] = (),
    excluded_anomaly_risks: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    enabled_meta_group_set = set(enabled_meta_groups)
    excluded_liq_set = set(excluded_liquidity_indicators)
    excluded_anomaly_set = set(excluded_anomaly_risks)
    return [
        row
        for row in overview_rows
        if get_meta_group_name(row) in enabled_meta_group_set
        and (not have_skills_only or skill_requirements_met(row))
        and (
            not positive_profit_only
            or ((row.get("profit_amount") is not None) and float(row.get("profit_amount") or 0.0) > 0.0)
        )
        and (
            float(row.get("profit_margin_fraction") or 0.0) >= (float(min_margin_pct or 0.0) / 100.0)
        )
        and float(row.get("isk_per_hour") or 0.0) >= float(min_isk_per_hour or 0.0)
        and int(row.get("region_daily_volume") or 0) >= int(min_region_daily_volume or 0)
        and str(row.get("liquidity_indicator") or "Unknown") not in excluded_liq_set
        and str(row.get("price_anomaly_risk") or "None") not in excluded_anomaly_set
    ]


@st.cache_data(ttl=300, show_spinner=False)
def build_overview_grid_frame(
    filtered_overview_rows: list[dict[str, Any]],
) -> tuple[pd.DataFrame, int, str]:
    tree_rows = flatten_overview_job_tree_rows(filtered_overview_rows)
    df = pd.DataFrame(tree_rows).reset_index(drop=True)
    path_fingerprint = "|".join(str(row.get("_path") or "") for row in tree_rows)
    grid_state_key = hashlib.md5(path_fingerprint.encode("utf-8")).hexdigest() if path_fingerprint else "empty"
    height = min(1100, 120 + (len(tree_rows) * 34))
    return df, height, grid_state_key


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
        return "Unknown Character"
    if owner_type == "corporation":
        return "Unknown Corporation"
    if int(asset.get("character_id") or 0) > 0:
        return "Unknown Character"
    if int(asset.get("corporation_id") or 0) > 0:
        return "Unknown Corporation"
    return ""
