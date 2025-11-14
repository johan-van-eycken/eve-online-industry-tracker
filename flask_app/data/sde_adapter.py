import json
import re
from classes.database_models import Categories, Groups, Types, TypeMaterials, StaStation

_db_sde = None
_language = None

def sde_adapter(db):
    global _db_sde, _language
    _db_sde = db
    _language = db.language or "en"

def _ensure():
    if _db_sde is None:
        raise RuntimeError("SDE DB not initialized. Call init_sde(db_sde) first.")
    if _language is None:
        raise RuntimeError("Language not set in SDE adapter.")  

# -------- helpers --------
def _parse_localized(raw):
    if raw is None:
        return ""
    if isinstance(raw, dict):
        text = raw.get(_language) or next(iter(raw.values()), "")
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                text = data.get(_language) or next(iter(data.values()), raw)
            return raw
        except json.JSONDecodeError:
            return raw
    
    # Clean HTML tags if any
    clean = re.sub(r'<[^>]+>', '', text).replace('\r\n', '<br>').strip()

    return clean

# -------- ores --------
def get_all_ores():
    _ensure()

    groups = _db_sde.session.query(Groups).filter(Groups.published == 1, Groups.categoryID == 25).all()
    if not groups:
        return []
    group_ids = [g.id for g in groups]

    type_q = _db_sde.session.query(Types).filter(Types.published == 1, Types.groupID.in_(group_ids)).all()
    if not type_q:
        return []

    ores = []
    for t in type_q:
        type_group = _db_sde.session.query(Groups).filter(Groups.id == t.groupID).first()
        type_category = _db_sde.session.query(Categories).filter(Categories.id == type_group.categoryID).first()
        type_mat_q = _db_sde.session.query(TypeMaterials).filter(TypeMaterials.id == t.id).all()

        ore = {
            "id": t.id,
            "name": _parse_localized(t.name) or str(t.id),
            "volume": t.volume,
            "portionSize": t.portionSize,
            "description": _parse_localized(t.description),
            "iconID": t.iconID,
            "groupID": t.groupID,
            "groupName": _parse_localized(type_group.name),
            "categoryID": type_group.categoryID,
            "categoryName": _parse_localized(type_category.name),
            "materials": []
        }
        for tm in type_mat_q:
            for mat in tm.materials:
                mat_type = _db_sde.session.query(Types).filter(Types.id == mat["materialTypeID"]).first()
                mat_group = _db_sde.session.query(Groups).filter(Groups.id == mat_type.groupID).first()
                mat_category = _db_sde.session.query(Categories).filter(Categories.id == mat_group.categoryID).first()

                ore["materials"].append({
                    "id": mat["materialTypeID"],
                    "name": _parse_localized(mat_type.name),
                    "volume": mat_type.volume,
                    "portionSize": mat_type.portionSize,
                    "description": _parse_localized(mat_type.description),
                    "iconId": mat_type.iconID,
                    "groupID": mat_type.groupID,
                    "groupName": _parse_localized(mat_group.name),
                    "categoryID": mat_group.categoryID,
                    "categoryName": _parse_localized(mat_category.name),
                    "quantity": mat["quantity"]
                })

        ores.append(ore)
    return ores

# -------- materials --------
def get_all_materials():
    """
    Returns list of base materials (groupID=18) with metadata.
    """
    _ensure()

    # Query full Types rows (need name etc.)
    material_rows = _db_sde.session.query(Types).filter(Types.published == 1, Types.groupID == 18, Types.metaGroupID == None).all()

    out = []
    for t in material_rows:
        out.append({
            "id": t.id,
            "name": _parse_localized(t.name) or str(t.id),
            "volume": getattr(t, "volume", 0.01),
            "basePrice": getattr(t, "basePrice", 0.0)
        })
    out.sort(key=lambda r: r["id"])
    return out
