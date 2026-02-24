from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.esi_service import ESIService

from eve_online_industry_tracker.infrastructure.persistence import blueprints_repo
from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data


def _model_to_dict(model_instance) -> dict[str, Any]:
    return {column.name: getattr(model_instance, column.name) for column in model_instance.__table__.columns}


def _normalize_asset_dict(asset_dict: dict[str, Any], source: str) -> dict[str, Any]:
    normalized = asset_dict.copy()

    if source == "character":
        normalized["owner_id"] = normalized.get("character_id")
        normalized["is_corporation"] = False
        normalized.pop("character_id", None)
    elif source == "corporation":
        normalized["owner_id"] = normalized.get("corporation_id")
        normalized["is_corporation"] = True
        normalized.pop("corporation_id", None)
    else:
        normalized["owner_id"] = normalized.get("character_id") or normalized.get("corporation_id")
        normalized["is_corporation"] = None
        normalized.pop("character_id", None)
        normalized.pop("corporation_id", None)

    columns_to_remove = [
        "location_flag",
        "location_type",
        "ship_name",
        "type_adjusted_price",
        "type_average_price",
        "type_capacity",
        "type_default_volume",
        "type_faction_description",
        "type_faction_id",
        "type_faction_name",
        "type_faction_short_description",
        "type_race_description",
        "type_race_id",
        "type_race_name",
        "type_repackaged_volume",
        "type_volume",
        "type_description",
    ]
    for col in columns_to_remove:
        normalized.pop(col, None)

    return normalized


def _extract_manufacturing_data(bp_info: dict[str, Any]) -> dict[str, Any]:
    manufacturing = bp_info.get("manufacturing", {})
    invention = bp_info.get("invention", {})
    return {
        "manufacturing_time": manufacturing.get("time", 0),
        "materials": manufacturing.get("materials", []),
        "products": manufacturing.get("products", []),
        "required_skills": manufacturing.get("skills", []),
        "invention_time": invention.get("time", 0),
        "invention_probability": invention.get("probability", None),
        "invention_materials": invention.get("materials", []),
        "invention_products": invention.get("products", []),
        "invention_required_skills": invention.get("skills", []),
        "research_time": bp_info.get("research_time", 0),
        "research_material": bp_info.get("research_material", 0),
        "copying_time": bp_info.get("copying", 0),
        "max_production_limit": bp_info.get("max_production_limit", 0),
    }


def _build_owned_blueprints(session) -> tuple[list[dict[str, Any]], set[int]]:
    char_blueprints = blueprints_repo.get_character_blueprints(session)
    corp_blueprints = blueprints_repo.get_corporation_blueprints(session)

    char_ids = {bp.character_id for bp in char_blueprints}
    corp_ids = {bp.corporation_id for bp in corp_blueprints}

    char_name_map = blueprints_repo.get_character_name_map(session, char_ids)
    corp_name_map = blueprints_repo.get_corporation_name_map(session, corp_ids)

    owned_blueprints: list[dict[str, Any]] = []
    owned_type_ids: set[int] = set()

    for bp in char_blueprints:
        bp_dict = _normalize_asset_dict(_model_to_dict(bp), "character")
        bp_dict["owned"] = True
        bp_dict["owner_name"] = char_name_map.get(bp.character_id, "Unknown")
        owned_blueprints.append(bp_dict)
        owned_type_ids.add(bp_dict["type_id"])

    for bp in corp_blueprints:
        bp_dict = _normalize_asset_dict(_model_to_dict(bp), "corporation")
        bp_dict["owned"] = True
        bp_dict["owner_name"] = corp_name_map.get(bp.corporation_id, "Unknown")
        owned_blueprints.append(bp_dict)
        owned_type_ids.add(bp_dict["type_id"])

    return owned_blueprints, owned_type_ids


def get_blueprint_assets(
    session,
    *,
    esi_service: ESIService | None,
    sde_session,
    language: str,
    include_unowned: bool = False,
) -> list[dict[str, Any]]:
    """Return blueprint assets enriched with SDE manufacturing data and ESI prices.

    Keeps the output payload stable for the existing API routes.
    """

    owned_blueprints, owned_type_ids = _build_owned_blueprints(session)

    all_blueprint_data = get_blueprint_manufacturing_data(
        sde_session,
        language,
        blueprint_type_ids=None if include_unowned else sorted(owned_type_ids),
    )

    result: list[dict[str, Any]] = []
    for bp in owned_blueprints:
        type_id = bp["type_id"]
        bp_info = all_blueprint_data.get(type_id, {})
        bp.update(_extract_manufacturing_data(bp_info))
        result.append(bp)

    unowned_type_ids = set(all_blueprint_data.keys()) - owned_type_ids
    if include_unowned and unowned_type_ids:
        for type_id in unowned_type_ids:
            bp_info = all_blueprint_data.get(type_id, {})
            bp: dict[str, Any] = {
                "type_id": type_id,
                "type_name": bp_info.get("type_name", None),
                "type_meta_group_id": bp_info.get("type_meta_group_id", None),
                "type_group_id": bp_info.get("group_id", None),
                "type_group_name": bp_info.get("group_name", None),
                "type_category_id": bp_info.get("category_id", None),
                "type_category_name": bp_info.get("category_name", None),
                "owner_id": None,
                "owner_name": None,
                "location_id": None,
                "item_id": None,
                "is_singleton": True,
                "is_corporation": False,
                "is_blueprint_copy": False,
                "container_name": None,
                "owned": False,
                "blueprint_material_efficiency": 0,
                "blueprint_time_efficiency": 0,
                "blueprint_runs": -1,
                "quantity": 0,
            }
            bp.update(_extract_manufacturing_data(bp_info))
            result.append(bp)

    market_prices: list[dict] = []
    try:
        if esi_service is not None:
            market_prices = esi_service.get_market_prices() or []
    except Exception:
        market_prices = []

    price_dict = {
        item["type_id"]: {
            "adjusted_price": item.get("adjusted_price"),
            "average_price": item.get("average_price"),
        }
        for item in market_prices
        if isinstance(item, dict) and "type_id" in item
    }

    for bp in result:
        for mat in bp.get("materials", []) or []:
            if not isinstance(mat, dict):
                continue
            mat_type_id = mat.get("type_id")
            if mat_type_id in price_dict:
                mat["adjusted_price"] = price_dict[mat_type_id].get("adjusted_price")
                mat["average_price"] = price_dict[mat_type_id].get("average_price")
            else:
                mat["adjusted_price"] = None
                mat["average_price"] = None

            unit = mat.get("adjusted_price")
            if unit is None or (isinstance(unit, (int, float)) and float(unit) <= 0):
                unit = mat.get("average_price")
            if unit is not None and isinstance(unit, (int, float)) and float(unit) <= 0:
                unit = None
            mat["unit_price"] = unit

        for prod in bp.get("products", []) or []:
            if not isinstance(prod, dict):
                continue
            prod_type_id = prod.get("type_id")
            if prod_type_id in price_dict:
                prod["adjusted_price"] = price_dict[prod_type_id].get("adjusted_price")
                prod["average_price"] = price_dict[prod_type_id].get("average_price")
            else:
                prod["adjusted_price"] = None
                prod["average_price"] = None

            unit = prod.get("average_price")
            if unit is None or (isinstance(unit, (int, float)) and float(unit) <= 0):
                unit = prod.get("adjusted_price")
            if unit is not None and isinstance(unit, (int, float)) and float(unit) <= 0:
                unit = None
            prod["unit_price"] = unit

    return result
