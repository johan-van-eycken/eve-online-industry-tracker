"""
Adapter for retrieving application data from the local database.
"""
from typing import Any, Dict, List

from classes.database_models import CharacterAssetsModel, CorporationAssetsModel
from flask_app.data.sde_adapter import get_blueprint_manufacturing_data

_db_app = None


def app_adapter(db_app):
    """Initialize the app adapter with database connection."""
    global _db_app
    _db_app = db_app


def _ensure():
    if _db_app is None:
        raise RuntimeError("App DB not initialized. Call app_adapter(db_app) first.")


def _model_to_dict(model_instance) -> Dict[str, Any]:
    """Convert SQLAlchemy model instance to dictionary."""
    return {
        column.name: getattr(model_instance, column.name)
        for column in model_instance.__table__.columns
    }


def _normalize_asset_dict(asset_dict: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Normalize asset dictionary to ensure consistent structure."""
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
        normalized["owner_id"] = normalized.get("character_id") or normalized.get(
            "corporation_id"
        )
        normalized["is_corporation"] = None
        normalized.pop("character_id", None)
        normalized.pop("corporation_id", None)

    # Remove unwanted columns
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


def _extract_manufacturing_data(bp_info: Dict[str, Any]) -> Dict[str, Any]:
    """Extract manufacturing data from blueprint info."""
    manufacturing = bp_info.get("manufacturing", {})
    
    return {
        "manufacturing_time": manufacturing.get("time", 0),
        "materials": manufacturing.get("materials", []),
        "products": manufacturing.get("products", []),
        "required_skills": manufacturing.get("skills", []),
        "research_time": bp_info.get("research_time", 0),
        "research_material": bp_info.get("research_material", 0),
        "copying_time": bp_info.get("copying", 0),
    }


def get_blueprint_assets() -> List[Dict[str, Any]]:
    """Get all blueprints with manufacturing data, enriched with ownership information."""
    _ensure()

    # Get owned blueprints first
    char_blueprints = (
        _db_app.session.query(CharacterAssetsModel)
        .filter(CharacterAssetsModel.type_category_name == "Blueprint")
        .all()
    )
    corp_blueprints = (
        _db_app.session.query(CorporationAssetsModel)
        .filter(CorporationAssetsModel.type_category_name == "Blueprint")
        .all()
    )

    # Convert to dicts and normalize
    owned_blueprints = []
    owned_type_ids = set()

    for bp in char_blueprints:
        bp_dict = _normalize_asset_dict(_model_to_dict(bp), "character")
        bp_dict["owned"] = True
        owned_blueprints.append(bp_dict)
        owned_type_ids.add(bp_dict["type_id"])

    for bp in corp_blueprints:
        bp_dict = _normalize_asset_dict(_model_to_dict(bp), "corporation")
        bp_dict["owned"] = True
        owned_blueprints.append(bp_dict)
        owned_type_ids.add(bp_dict["type_id"])

    # Get all blueprint manufacturing data from SDE
    all_blueprint_data = get_blueprint_manufacturing_data()

    # Enrich owned blueprints with manufacturing data
    result = []
    for bp in owned_blueprints:
        type_id = bp["type_id"]
        bp_info = all_blueprint_data.get(type_id, {})

        # Add manufacturing data
        bp.update(_extract_manufacturing_data(bp_info))

        result.append(bp)

    # Add unowned blueprints
    unowned_type_ids = set(all_blueprint_data.keys()) - owned_type_ids
    
    if unowned_type_ids:
        for type_id in unowned_type_ids:
            bp = {
                "type_id": type_id,
                "type_name": all_blueprint_data.get(type_id, {}).get("type_name", None),
                "type_meta_group_id": all_blueprint_data.get(type_id, {}).get("type_meta_group_id", None),
                "type_group_id": all_blueprint_data.get(type_id, {}).get("group_id", None),
                "type_group_name": all_blueprint_data.get(type_id, {}).get("group_name", None),
                "type_category_id": all_blueprint_data.get(type_id, {}).get("category_id", None),
                "type_category_name": all_blueprint_data.get(type_id, {}).get("category_name", None),
                "owner_id": None,
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
                "quantity": 0
            }
            
            # Add manufacturing data
            bp.update(_extract_manufacturing_data(bp_info))

            result.append(bp)

    return result