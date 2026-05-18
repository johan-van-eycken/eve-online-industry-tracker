from __future__ import annotations

from typing import Any

import pandas as pd


ASSET_DISPLAY_COLUMNS = [
    "image_url",
    "type_name",
    "quantity",
    "type_volume",
    "total_volume",
    "acquisition_source",
    "acquisition_unit_cost",
    "acquisition_total_cost",
    "acquisition_date",
    "type_average_price",
    "total_average_price",
    "type_group_name",
    "type_category_name",
]

ASSET_ISK_COLUMNS = [
    "acquisition_unit_cost",
    "acquisition_total_cost",
    "type_average_price",
    "total_average_price",
]

ASSET_INT_COLUMNS = ["quantity"]
ASSET_FLOAT_COLUMNS = ["type_volume", "total_volume"]


def safe_int(value: Any, default: int = 0) -> int:
    if pd.isna(value):
        return default
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    if pd.isna(value):
        return default
    try:
        return float(value)
    except Exception:
        return default


def get_item_image_url(*, type_id: int | str | None, type_category_name: str = "", is_blueprint_copy: bool = False, size: int = 32) -> str:
    if type_category_name == "Blueprint":
        variation = "bpc" if is_blueprint_copy else "bp"
    elif type_category_name == "Permanent SKIN":
        variation = "skins"
    else:
        variation = "icon"
    return f"https://images.evetech.net/types/{type_id}/{variation}?size={int(size)}"


def apply_location_names_from_data(
    df: pd.DataFrame,
    location_data: dict[str, Any],
    *,
    top_location_column: str = "top_location_id",
    location_name_column: str = "location_name",
) -> tuple[pd.DataFrame, dict[Any, str]]:
    if df.empty or top_location_column not in df.columns:
        return df.copy(), {}

    out = df.copy()
    location_ids = list(out[top_location_column].dropna().unique())

    for loc_id in location_ids:
        location_info = location_data.get(str(loc_id)) or {}
        location_name = location_info.get("name", str(loc_id))
        out.loc[out[top_location_column] == loc_id, location_name_column] = location_name

    location_names: dict[Any, str] = {}
    for location_id in location_ids:
        if location_name_column not in out.columns:
            location_names[location_id] = str(location_id)
            continue
        subset = out[out[top_location_column] == location_id][location_name_column].dropna()
        location_names[location_id] = subset.iloc[0] if not subset.empty else str(location_id)

    return out, location_names


def add_item_images(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()

    def get_variation(row: pd.Series) -> str:
        category_name = row.get("type_category_name")
        if category_name == "Blueprint":
            return "bpc" if bool(row.get("is_blueprint_copy")) else "bp"
        if category_name == "Permanent SKIN":
            return "skins"
        return "icon"

    out["image_variation"] = out.apply(get_variation, axis=1)
    out["image_url"] = out.apply(
        lambda row: get_item_image_url(
            type_id=row.get("type_id"),
            type_category_name=str(row.get("type_category_name") or ""),
            is_blueprint_copy=bool(row.get("is_blueprint_copy")),
            size=32,
        ),
        axis=1,
    )
    return out


def summarize_asset_items(df: pd.DataFrame) -> tuple[int, float, float]:
    if df.empty:
        return 0, 0.0, 0.0

    unique_items = int(df["type_name"].nunique()) if "type_name" in df.columns else 0
    total_volume = 0.0
    total_value = 0.0

    if {"type_volume", "quantity"}.issubset(df.columns):
        total_volume = float((df["type_volume"] * df["quantity"]).sum())
    if {"type_average_price", "quantity"}.issubset(df.columns):
        total_value = float((df["type_average_price"] * df["quantity"]).sum())

    return unique_items, total_volume, total_value


def build_asset_display_frame(
    df: pd.DataFrame,
    *,
    prefer_container_names: bool = False,
    prefer_ship_names: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = add_item_images(df)

    if {"type_volume", "quantity"}.issubset(out.columns):
        out["total_volume"] = out["type_volume"] * out["quantity"]
    if {"type_average_price", "quantity"}.issubset(out.columns):
        out["total_average_price"] = out["type_average_price"] * out["quantity"]

    if prefer_container_names and "container_name" in out.columns and out["container_name"].notnull().all():
        out["type_name"] = out["container_name"]
    if prefer_ship_names and "ship_name" in out.columns and out["ship_name"].notnull().all():
        out["type_name"] = out["ship_name"]

    display_columns = [column for column in ASSET_DISPLAY_COLUMNS if column in out.columns]
    if not display_columns:
        return out.copy()

    return out.filter(items=display_columns).sort_values(by=["type_name"])


def ship_icon_filename(group_id: int) -> str:
    if group_id in [25, 324, 541, 830, 831, 834, 893, 1022, 1283, 1527]:
        return "frigate_16.png"
    if group_id in [420, 1305, 1534]:
        return "destroyer_16.png"
    if group_id in [26, 358, 832, 833, 894, 906, 963, 1972]:
        return "cruiser_16.png"
    if group_id in [419, 540, 1201]:
        return "battleCruiser_16.png"
    if group_id in [27, 381, 898, 900]:
        return "battleship_16.png"
    if group_id in [485, 4594]:
        return "dreadnought_16.png"
    if group_id in [547, 659, 1538]:
        return "carrier_16.png"
    if group_id == 30:
        return "titan_16.png"
    if group_id in [28, 380, 1202]:
        return "industrial_16.png"
    if group_id == 941:
        return "industrialCommand_16.png"
    if group_id in [513, 883, 902]:
        return "freighter_16.png"
    if group_id in [463, 543]:
        return "miningBarge_16.png"
    if group_id == 29:
        return "capsule_16.png"
    if group_id == 31:
        return "shuttle_16.png"
    if group_id == 237:
        return "rookie_16.png"
    return "ship_16.png"


def ship_overlay_icon_url(meta_group_id: int) -> str | None:
    from flask_app.settings import api_base
    base_url = f"{api_base()}/static/images/icons/overlay/"
    if meta_group_id == 2:
        return f"{base_url}tech_2.png"
    if meta_group_id == 3:
        return f"{base_url}tech_3.png"
    if meta_group_id == 4:
        return f"{base_url}tech_faction.png"
    return None