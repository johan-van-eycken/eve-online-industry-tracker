from __future__ import annotations

from typing import Dict, List

from flask_app.persistence import sde_static_repo
from flask_app.services.sde_localization import parse_localized


def build_all_ores(session, language: str) -> List[dict]:
    groups = sde_static_repo.get_ore_groups(session)
    if not groups:
        return []

    group_ids = [g.id for g in groups]
    ore_types = sde_static_repo.get_published_types_by_group_ids(session, group_ids)
    if not ore_types:
        return []

    group_map = {g.id: g for g in sde_static_repo.get_groups_by_ids(session, {t.groupID for t in ore_types})}
    category_map = {
        c.id: c
        for c in sde_static_repo.get_categories_by_ids(
            session,
            {getattr(group_map.get(t.groupID), "categoryID", None) for t in ore_types},
        )
    }

    type_material_rows = sde_static_repo.get_type_materials_by_type_ids(session, {t.id for t in ore_types})
    type_materials_map = {tm.id: tm for tm in type_material_rows}

    # Collect all material type IDs to batch load types/groups/categories.
    material_type_ids = set()
    for tm in type_material_rows:
        for mat in getattr(tm, "materials", []) or []:
            if "materialTypeID" in mat:
                material_type_ids.add(mat["materialTypeID"])

    material_types = sde_static_repo.get_types_by_ids(session, material_type_ids)
    material_type_map = {t.id: t for t in material_types}

    material_group_map = {
        g.id: g
        for g in sde_static_repo.get_groups_by_ids(session, {t.groupID for t in material_types})
    }
    material_category_map = {
        c.id: c
        for c in sde_static_repo.get_categories_by_ids(
            session,
            {getattr(material_group_map.get(t.groupID), "categoryID", None) for t in material_types},
        )
    }

    ores: List[dict] = []

    for t in ore_types:
        type_group = group_map.get(t.groupID)
        type_category = category_map.get(getattr(type_group, "categoryID", None))
        type_mat_row = type_materials_map.get(t.id)

        ore = {
            "id": t.id,
            "name": parse_localized(t.name, language) or str(t.id),
            "volume": t.volume,
            "portionSize": t.portionSize,
            "description": parse_localized(t.description, language),
            "iconID": t.iconID,
            "groupID": t.groupID,
            "groupName": parse_localized(getattr(type_group, "name", ""), language),
            "categoryID": getattr(type_group, "categoryID", None),
            "categoryName": parse_localized(getattr(type_category, "name", ""), language),
            "materials": [],
        }

        for mat in getattr(type_mat_row, "materials", []) or []:
            mat_type_id = mat.get("materialTypeID")
            mat_type = material_type_map.get(mat_type_id)
            mat_group = material_group_map.get(getattr(mat_type, "groupID", None))
            mat_category = material_category_map.get(getattr(mat_group, "categoryID", None))

            ore["materials"].append(
                {
                    "id": mat_type_id,
                    "name": parse_localized(getattr(mat_type, "name", ""), language),
                    "volume": getattr(mat_type, "volume", None),
                    "portionSize": getattr(mat_type, "portionSize", None),
                    "description": parse_localized(getattr(mat_type, "description", ""), language),
                    "iconId": getattr(mat_type, "iconID", None),
                    "groupID": getattr(mat_type, "groupID", None),
                    "groupName": parse_localized(getattr(mat_group, "name", ""), language),
                    "categoryID": getattr(mat_group, "categoryID", None),
                    "categoryName": parse_localized(getattr(mat_category, "name", ""), language),
                    "quantity": mat.get("quantity"),
                }
            )

        ores.append(ore)

    return ores


def build_all_materials(session, language: str) -> List[dict]:
    material_rows = sde_static_repo.get_base_material_types(session)

    out: List[dict] = []
    for t in material_rows:
        out.append(
            {
                "id": t.id,
                "name": parse_localized(t.name, language) or str(t.id),
                "volume": getattr(t, "volume", 0.01),
                "basePrice": getattr(t, "basePrice", 0.0),
            }
        )

    out.sort(key=lambda r: r["id"])
    return out
