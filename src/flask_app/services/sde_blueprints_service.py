from __future__ import annotations

from typing import Dict

from classes.database_models import Blueprints

from flask_app.services.sde_types_service import get_type_data


def get_blueprint_manufacturing_data(session, language: str) -> Dict[int, Dict]:
    """Return manufacturing materials/products/skills and research times for all blueprints."""

    blueprints = session.query(Blueprints).all()
    if not blueprints:
        return {}

    blueprint_type_ids = set()
    material_type_ids = set()
    product_type_ids = set()
    skill_type_ids = set()

    for bp in blueprints:
        blueprint_type_ids.add(bp.blueprintTypeID)
        activities = bp.activities if isinstance(bp.activities, dict) else {}
        manufacturing = activities.get("manufacturing", {})

        for mat in manufacturing.get("materials", []):
            material_type_ids.add(mat["typeID"])

        for prod in manufacturing.get("products", []):
            product_type_ids.add(prod["typeID"])

        for skill in manufacturing.get("skills", []):
            skill_type_ids.add(skill["typeID"])

    all_type_ids = list(material_type_ids | product_type_ids | blueprint_type_ids | skill_type_ids)
    type_data_map = get_type_data(session, language, all_type_ids)

    result: Dict[int, Dict] = {}

    for bp in blueprints:
        activities = bp.activities if isinstance(bp.activities, dict) else {}
        manufacturing = activities.get("manufacturing", {})

        materials = []
        for mat in manufacturing.get("materials", []):
            type_id = mat.get("typeID")
            type_data = type_data_map.get(type_id, {})
            materials.append(
                {
                    "type_id": type_id,
                    "type_name": type_data.get("type_name", ""),
                    "group_id": type_data.get("group_id"),
                    "group_name": type_data.get("group_name", ""),
                    "category_id": type_data.get("category_id"),
                    "category_name": type_data.get("category_name", ""),
                    "quantity": mat["quantity"],
                }
            )

        products = []
        for prod in manufacturing.get("products", []):
            type_id = prod.get("typeID")
            type_data = type_data_map.get(type_id, {})
            products.append(
                {
                    "type_id": type_id,
                    "type_name": type_data.get("type_name", ""),
                    "group_id": type_data.get("group_id"),
                    "group_name": type_data.get("group_name", ""),
                    "category_id": type_data.get("category_id"),
                    "category_name": type_data.get("category_name", ""),
                    "quantity": prod["quantity"],
                }
            )

        skills = []
        for skill in manufacturing.get("skills", []):
            type_id = skill.get("typeID")
            type_data = type_data_map.get(type_id, {})
            skills.append(
                {
                    "type_id": type_id,
                    "type_name": type_data.get("type_name", ""),
                    "group_id": type_data.get("group_id"),
                    "group_name": type_data.get("group_name", ""),
                    "category_id": type_data.get("category_id"),
                    "category_name": type_data.get("category_name", ""),
                    "level": skill["level"],
                }
            )

        type_id = bp.blueprintTypeID
        type_data = type_data_map.get(type_id, {})

        result[bp.blueprintTypeID] = {
            "type_id": type_id,
            "type_name": type_data.get("type_name", ""),
            "type_meta_group_id": type_data.get("meta_group_id"),
            "group_id": type_data.get("group_id"),
            "group_name": type_data.get("group_name", ""),
            "category_id": type_data.get("category_id"),
            "category_name": type_data.get("category_name", ""),
            "manufacturing": {
                "time": manufacturing.get("time", 0),
                "materials": materials,
                "products": products,
                "skills": skills,
            },
            "research_time": activities.get("research_time", {}).get("time", 0),
            "research_material": activities.get("research_material", {}).get("time", 0),
            "copying": activities.get("copying", {}).get("time", 0),
        }

    return result
