from __future__ import annotations

from typing import Iterable

from eve_online_industry_tracker.db_models import Blueprints

from eve_online_industry_tracker.infrastructure.sde.types import get_type_data


def get_blueprint_manufacturing_data(
    session,
    language: str,
    blueprint_type_ids: Iterable[int] | None = None,
) -> dict[int, dict]:
    """Return manufacturing materials/products/skills and research times for blueprints.

    If `blueprint_type_ids` is provided, only those blueprint typeIDs are loaded.
    """

    q = session.query(Blueprints)
    if blueprint_type_ids is not None:
        ids = list({int(i) for i in blueprint_type_ids if i is not None})
        if not ids:
            return {}
        q = q.filter(Blueprints.blueprintTypeID.in_(ids))

    blueprints = q.all()
    if not blueprints:
        return {}

    blueprint_type_id_set: set[int] = set()
    material_type_ids: set[int] = set()
    product_type_ids: set[int] = set()
    skill_type_ids: set[int] = set()

    for bp in blueprints:
        blueprint_type_id_set.add(bp.blueprintTypeID)
        activities = bp.activities if isinstance(bp.activities, dict) else {}
        manufacturing = activities.get("manufacturing", {})

        invention = activities.get("invention", {})

        for mat in manufacturing.get("materials", []):
            try:
                material_type_ids.add(int(mat["typeID"]))
            except Exception:
                continue

        for prod in manufacturing.get("products", []):
            try:
                product_type_ids.add(int(prod["typeID"]))
            except Exception:
                continue

        for skill in manufacturing.get("skills", []):
            try:
                skill_type_ids.add(int(skill["typeID"]))
            except Exception:
                continue

        # Invention activity (T2 invention from T1 BPCs)
        if isinstance(invention, dict):
            for mat in invention.get("materials", []):
                try:
                    material_type_ids.add(int(mat["typeID"]))
                except Exception:
                    continue

            for prod in invention.get("products", []):
                try:
                    product_type_ids.add(int(prod["typeID"]))
                except Exception:
                    continue

            for skill in invention.get("skills", []):
                try:
                    skill_type_ids.add(int(skill["typeID"]))
                except Exception:
                    continue

    all_type_ids = list(material_type_ids | product_type_ids | blueprint_type_id_set | skill_type_ids)
    type_data_map = get_type_data(session, language, all_type_ids)

    result: dict[int, dict] = {}

    for bp in blueprints:
        activities = bp.activities if isinstance(bp.activities, dict) else {}
        manufacturing = activities.get("manufacturing", {})

        invention = activities.get("invention", {}) if isinstance(activities.get("invention", {}), dict) else {}

        materials = []
        for mat in manufacturing.get("materials", []):
            raw_type_id = mat.get("typeID")
            try:
                type_id = int(raw_type_id)
            except Exception:
                continue
            type_data = type_data_map.get(int(type_id), {})
            materials.append(
                {
                    "type_id": int(type_id),
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
            raw_type_id = prod.get("typeID")
            try:
                type_id = int(raw_type_id)
            except Exception:
                continue
            type_data = type_data_map.get(int(type_id), {})
            products.append(
                {
                    "type_id": int(type_id),
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
            raw_type_id = skill.get("typeID")
            try:
                type_id = int(raw_type_id)
            except Exception:
                continue
            type_data = type_data_map.get(int(type_id), {})
            skills.append(
                {
                    "type_id": int(type_id),
                    "type_name": type_data.get("type_name", ""),
                    "group_id": type_data.get("group_id"),
                    "group_name": type_data.get("group_name", ""),
                    "category_id": type_data.get("category_id"),
                    "category_name": type_data.get("category_name", ""),
                    "level": skill["level"],
                }
            )

        invention_materials = []
        for mat in (invention.get("materials", []) or []):
            if not isinstance(mat, dict):
                continue
            raw_type_id = mat.get("typeID")
            try:
                type_id = int(raw_type_id)
            except Exception:
                continue
            type_data = type_data_map.get(int(type_id), {})
            invention_materials.append(
                {
                    "type_id": int(type_id),
                    "type_name": type_data.get("type_name", ""),
                    "group_id": type_data.get("group_id"),
                    "group_name": type_data.get("group_name", ""),
                    "category_id": type_data.get("category_id"),
                    "category_name": type_data.get("category_name", ""),
                    "quantity": mat.get("quantity"),
                }
            )

        invention_products = []
        for prod in (invention.get("products", []) or []):
            if not isinstance(prod, dict):
                continue
            raw_type_id = prod.get("typeID")
            try:
                type_id = int(raw_type_id)
            except Exception:
                continue
            type_data = type_data_map.get(int(type_id), {})
            invention_products.append(
                {
                    "type_id": int(type_id),
                    "type_name": type_data.get("type_name", ""),
                    "group_id": type_data.get("group_id"),
                    "group_name": type_data.get("group_name", ""),
                    "category_id": type_data.get("category_id"),
                    "category_name": type_data.get("category_name", ""),
                    "probability": prod.get("probability"),
                    "quantity": prod.get("quantity"),
                }
            )

        invention_probability = invention.get("probability", None)
        if invention_probability is None:
            # Some SDE exports store invention chance on the product entry instead of
            # as a top-level invention.probability.
            raw_probs: list[float] = []
            for prod in (invention.get("products", []) or []):
                if not isinstance(prod, dict):
                    continue
                p = prod.get("probability")
                if p is None:
                    continue
                try:
                    pf = float(p)
                except Exception:
                    continue
                if pf > 0:
                    raw_probs.append(pf)

            if len(raw_probs) == 1:
                invention_probability = raw_probs[0]
            elif len(raw_probs) > 1:
                try:
                    if max(raw_probs) - min(raw_probs) < 1e-9:
                        invention_probability = raw_probs[0]
                except Exception:
                    pass

        invention_skills = []
        for skill in (invention.get("skills", []) or []):
            if not isinstance(skill, dict):
                continue
            raw_type_id = skill.get("typeID")
            try:
                type_id = int(raw_type_id)
            except Exception:
                continue
            type_data = type_data_map.get(int(type_id), {})
            invention_skills.append(
                {
                    "type_id": int(type_id),
                    "type_name": type_data.get("type_name", ""),
                    "group_id": type_data.get("group_id"),
                    "group_name": type_data.get("group_name", ""),
                    "category_id": type_data.get("category_id"),
                    "category_name": type_data.get("category_name", ""),
                    "level": skill.get("level"),
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
            "max_production_limit": int(getattr(bp, "maxProductionLimit", 0) or 0),
            "manufacturing": {
                "time": manufacturing.get("time", 0),
                "materials": materials,
                "products": products,
                "skills": skills,
            },
            "invention": {
                "time": invention.get("time", 0),
                "probability": invention_probability,
                "materials": invention_materials,
                "products": invention_products,
                "skills": invention_skills,
            },
            "research_time": activities.get("research_time", {}).get("time", 0),
            "research_material": activities.get("research_material", {}).get("time", 0),
            "copying": activities.get("copying", {}).get("time", 0),
        }

    return result
