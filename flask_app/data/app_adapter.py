"""
Adapter for retrieving application data from the local database.
"""
from datetime import datetime
from typing import Any, Dict, List

from classes.database_models import (
    CharacterModel, CharacterAssetsModel,
    CorporationModel, CorporationAssetsModel,
    IndustryProfilesModel
)
from flask_app.data.sde_adapter import get_blueprint_manufacturing_data
from flask_app.data.esi_adapter import get_market_prices

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
    """Get all blueprints with manufacturing data, enriched with ownership information and prices."""
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
    # Build owner name lookups
    char_ids = {bp.character_id for bp in char_blueprints}
    corp_ids = {bp.corporation_id for bp in corp_blueprints}

    char_name_map = {}
    if char_ids:
        characters = (_db_app.session.query(CharacterModel).filter(CharacterModel.character_id.in_(char_ids)).all())
        char_name_map = {char.character_id: char.character_name for char in characters}
    corp_name_map = {}
    if corp_ids:
        corporations = (_db_app.session.query(CorporationModel).filter(CorporationModel.corporation_id.in_(corp_ids)).all())
        corp_name_map = {corp.corporation_id: corp.corporation_name for corp in corporations}

    # Convert to dicts and normalize
    owned_blueprints = []
    owned_type_ids = set()

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
                "quantity": 0
            }
            
            # Add manufacturing data
            bp.update(_extract_manufacturing_data(bp_info))

            result.append(bp)

    # Fetch market prices for all involved items
    market_prices = get_market_prices()
    price_dict = {
        item["type_id"]: {
            "adjusted_price": item.get("adjusted_price"),
            "average_price": item.get("average_price"),
        }
        for item in market_prices
        if "type_id" in item
    }

    # Add prices for materials and products
    for bp in result:
        for mat in bp.get("materials", []):
            mat_type_id = mat.get("type_id")
            if mat_type_id in price_dict:
                mat["adjusted_price"] = price_dict[mat_type_id].get("adjusted_price")
                mat["average_price"] = price_dict[mat_type_id].get("average_price")
            else:
                mat["adjusted_price"] = None
                mat["average_price"] = None

        for prod in bp.get("products", []):
            prod_type_id = prod.get("type_id")
            if prod_type_id in price_dict:
                prod["adjusted_price"] = price_dict[prod_type_id].get("adjusted_price")
                prod["average_price"] = price_dict[prod_type_id].get("average_price")
            else:
                prod["adjusted_price"] = None
                prod["average_price"] = None

    return result

def get_industry_profiles(character_id:int) -> List[Dict[str, Any]]:
    """Get all industry profiles for a character."""
    _ensure()
    profiles = _db_app.session.query(IndustryProfilesModel).filter(
        IndustryProfilesModel.character_id == character_id
    ).all()
    return [_model_to_dict(profile) for profile in profiles]

def add_industry_profile(data: Dict[str, Any]) -> Dict[str, Any]:
    """Add a new industry profile."""
    _ensure()
    profile = IndustryProfilesModel(
        character_id=data["character_id"],
        profile_name=data["profile_name"],
        is_default=data.get("is_default", False),
        region_id=data.get("region_id"),
        system_id=data.get("system_id"),
        facility_id=data.get("facility_id"),
        facility_type=data.get("facility_type"),
        facility_tax=data.get("facility_tax"),
        material_efficiency_bonus=data.get("material_efficiency_bonus"),
        time_efficiency_bonus=data.get("time_efficiency_bonus"),
        rig_slot0_type_id=data.get("rig_slot0_type_id"),
        rig_slot1_type_id=data.get("rig_slot1_type_id"),
        rig_slot2_type_id=data.get("rig_slot2_type_id")
    )
    _db_app.session.add(profile)
    _db_app.session.commit()

    return profile.id

def edit_industry_profile(profile_id: int, data: Dict[str, Any]) -> None:
    """Edit an existing industry profile."""
    _ensure()
    profile = _db_app.session.query(IndustryProfilesModel).filter(
        IndustryProfilesModel.id == profile_id
    ).first()
    if not profile:
        raise ValueError(f"Industry profile with id {profile_id} not found.")

    # If this is set as default, unset all other defaults for this character
    if data.get("is_default", False):
        _db_app.session.query(IndustryProfilesModel).filter(
            IndustryProfilesModel.character_id == profile.character_id,
            IndustryProfilesModel.id != profile_id
        ).update({"is_default": False})

    # Update fields
    for field in [
        "profile_name", "is_default", "region_id", "system_id", "facility_id",
        "facility_type", "facility_tax", "material_efficiency_bonus", "time_efficiency_bonus",
        "rig_slot0_type_id", "rig_slot1_type_id", "rig_slot2_type_id"
    ]:
        if field in data:
            setattr(profile, field, data[field])
    
    profile.updated_at = datetime.now()
    _db_app.session.commit()
    return profile_id

def remove_industry_profile(profile_id: int) -> None:
    """Remove an industry profile."""
    _ensure()
    profile = _db_app.session.query(IndustryProfilesModel).filter(
        IndustryProfilesModel.id == profile_id
    ).first()
    if not profile:
        raise ValueError(f"Industry profile with id {profile_id} not found.")

    _db_app.session.delete(profile)
    _db_app.session.commit()
    return