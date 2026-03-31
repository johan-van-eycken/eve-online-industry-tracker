from __future__ import annotations

from typing import Any

import streamlit as st
import pandas as pd

from utils.assets_data import (
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
from utils.flask_api import api_post


@st.cache_data(ttl=3600)
def fetch_location_info(location_ids: list[int]) -> dict[str, Any]:
    try:
        response = api_post("/locations", payload={"location_ids": list(map(int, location_ids))})
        return response if isinstance(response, dict) else {}
    except Exception as e:
        return {"status": "error", "message": str(e), "data": {}}


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
            ship_icon = f"http://localhost:5000/static/images/icons/ships/{ship_icon_filename(group_id)}"
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
