from __future__ import annotations

import logging
from typing import Any

import streamlit as st
import pandas as pd

from streamlit_ui.components.assets_data import (
    ASSET_DISPLAY_COLUMNS,
    ASSET_FLOAT_COLUMNS,
    ASSET_INT_COLUMNS,
    ASSET_ISK_COLUMNS,
    add_item_images,
    apply_location_names_from_data,
    build_asset_display_frame,
    get_item_image_url,
    safe_float,
    safe_int,
    ship_icon_filename,
    ship_overlay_icon_url,
    summarize_asset_items,
)
from streamlit_ui.api.client import api_post
from flask_app.settings import api_base as _get_api_base


class _LocationLookupFailed(RuntimeError):
    """Raised inside the cached fetch so st.cache_data does not keep the error."""


@st.cache_data(ttl=300)
def _fetch_location_info_cached(location_ids: list[int]) -> dict[str, Any]:
    response = api_post("/locations", payload={"location_ids": list(map(int, location_ids))})
    if not isinstance(response, dict):
        # api_post logged the HTTP failure; give the page a reason to show.
        raise _LocationLookupFailed("the /locations request failed (see the logs)")
    if response.get("status") == "error":
        raise _LocationLookupFailed(str(response.get("message") or "the /locations request failed"))
    return response


def fetch_location_info(location_ids: list[int]) -> dict[str, Any]:
    """The /locations response; a failure is an error dict with the reason,
    which is not cached (the next run asks again)."""
    try:
        return _fetch_location_info_cached(location_ids)
    except _LocationLookupFailed as exc:
        return {"status": "error", "message": str(exc), "data": {}}


fetch_location_info.clear = _fetch_location_info_cached.clear  # type: ignore[attr-defined]


def apply_location_names(
    df: pd.DataFrame,
    *,
    top_location_column: str = "top_location_id",
    location_name_column: str = "location_name",
) -> tuple[pd.DataFrame, dict[Any, str]]:
    if df.empty or top_location_column not in df.columns:
        return df.copy(), {}

    location_ids = list(df[top_location_column].dropna().unique())
    location_info_map = fetch_location_info([int(location_id) for location_id in location_ids])
    if (location_info_map or {}).get("status") == "error":
        reason = location_info_map.get("message") or "unknown error"
        logging.warning("Location names could not be loaded: %s", reason)
        st.warning(f"Location names could not be loaded ({reason}); showing location IDs instead.")
    return apply_location_names_from_data(
        df,
        (location_info_map or {}).get("data", {}),
        top_location_column=top_location_column,
        location_name_column=location_name_column,
    )


def render_ship_cards(ships: pd.DataFrame, *, cards_per_row: int = 4) -> None:
    for row_start in range(0, len(ships), cards_per_row):
        columns = st.columns(cards_per_row)
        for offset, column in enumerate(columns):
            if row_start + offset >= len(ships):
                break

            ship = ships.iloc[row_start + offset]
            type_id = safe_int(ship.get("type_id"))
            faction_id = safe_int(ship.get("type_faction_id"))
            group_id = safe_int(ship.get("type_group_id"))
            meta_group_id = safe_int(ship.get("type_meta_group_id"))
            custom_name = ship.get("ship_name", "No Custom Name")
            ingame_name = ship.get("type_name", "Unknown")
            image_url = f"https://images.evetech.net/types/{type_id}/render?size=128"
            faction_url = f"https://images.evetech.net/corporations/{faction_id}/logo?size=64"
            ship_icon = f"{_get_api_base()}/static/images/icons/ships/{ship_icon_filename(group_id)}"
            ship_overlay_icon = ship_overlay_icon_url(meta_group_id)
            quantity = safe_int(ship.get("quantity"), default=1)
            quantity_label = f"x{quantity} {'Packaged' if not ship.get('is_singleton', False) else ''}"
            estimated_value = safe_float(ship.get("type_average_price")) * safe_float(ship.get("quantity"))
            total_volume = safe_float(ship.get("type_volume")) * safe_float(ship.get("quantity"))

            with column:
                st.markdown(
                    f"""
                    <div class="tooltip ship-tile" style="display: flex; align-items: center; background-color: rgba(30,30,30,0.95); padding: 0px; border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.6); margin-bottom: 10px; background-image: url('{faction_url}'); background-size: 64px 64px; background-repeat: no-repeat; background-position: 80% top; background-blend-mode: darken;">
                        <img src="{image_url}" width="96" style="border-radius:8px; margin-right:18px;" />
                        {f'<img src="{ship_overlay_icon}" style="position: absolute; top: 0px; left: 0px; width: 24px; height: 24px; border-radius:6px;" />' if ship_overlay_icon else '&nbsp;'}
                        <div style="flex:1; color:#f0f0f0;">
                            <div style="font-size:14px; color:#b0b0b0;">
                                <img src="{ship_icon}" width="16" style="border-radius:6px; margin-right:4px;" />
                                {ship.get('type_group_name', 'Unknown')}
                            </div>
                            <div style="font-size:16px; font-weight:bold; margin-top:4px;">{custom_name if custom_name is not None else ingame_name}</div>
                            <div style="font-size:14px; color:#b0b0b0; margin-top:1px;">{ingame_name if custom_name is not None else '&nbsp;'}</div>
                        </div>
                        <span style="position: absolute; bottom: 8px; right: 12px; background: rgba(0,0,0,0.85); font-size: 14px; font-weight: bold; padding: 2px 8px; border-radius: 8px; z-index: 2; box-shadow: 0 1px 4px rgba(0,0,0,0.4);">{quantity_label}</span>
                        <span class="tooltiptext">
                            {custom_name if custom_name is not None else ingame_name}<br />
                            <br />
                            Est. Value: {estimated_value:,.2f} ISK<br />
                            Volume: {total_volume:,.2f} m³
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def render_asset_dataframe(df: pd.DataFrame, *, height: int, prefer_container_names: bool = False, prefer_ship_names: bool = False) -> None:
    """Render assets via st.dataframe — works reliably inside st.expander."""
    df_display = build_asset_display_frame(df, prefer_container_names=prefer_container_names, prefer_ship_names=prefer_ship_names)
    if df_display.empty:
        st.info("No items.")
        return

    column_config: dict[str, Any] = {}
    if "image_url" in df_display.columns:
        column_config["image_url"] = st.column_config.ImageColumn("", width=40)
    for col in ASSET_ISK_COLUMNS:
        if col in df_display.columns:
            column_config[col] = st.column_config.NumberColumn(format="%.2f ISK")
    for col in ASSET_INT_COLUMNS:
        if col in df_display.columns:
            column_config[col] = st.column_config.NumberColumn(format="%d")
    for col in ASSET_FLOAT_COLUMNS:
        if col in df_display.columns:
            column_config[col] = st.column_config.NumberColumn(format="%.2f")

    st.dataframe(
        df_display,
        column_config=column_config,
        height=height,
        width="stretch",
        hide_index=True,
    )


def render_asset_summary(df: pd.DataFrame) -> None:
    unique_items, total_volume, total_value = summarize_asset_items(df)
    st.markdown(
        f"Items: {unique_items} - Total Volume: {total_volume:,.2f} m³ - Total Value: {total_value:,.2f} ISK"
    )
