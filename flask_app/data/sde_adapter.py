import json
from typing import List, Optional, Dict, Any
from classes.database_models import Groups, Types, TypeMaterials

_db_sde = None

def init_sde(db):
    global _db_sde
    _db_sde = db

# -------- helpers --------
def _parse_localized(raw, lang="en"):
    if raw is None:
        return ""
    if isinstance(raw, dict):
        return raw.get(lang) or next(iter(raw.values()), "")
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data.get(lang) or next(iter(data.values()), raw)
            return raw
        except json.JSONDecodeError:
            return raw
    return str(raw)

def _resolve_type_names(session, type_ids: List[int], lang="en") -> Dict[int, str]:
    if not type_ids:
        return {}
    rows = session.query(Types.id, Types.name).filter(Types.id.in_(type_ids)).all()
    return {tid: (_parse_localized(nm, lang) or str(tid)) for tid, nm in rows}

# -------- ores --------
def get_all_ores(ore_ids: Optional[List[int]] = None, lang: str = "en"):
    if _db_sde is None:
        raise RuntimeError("SDE DB not initialized. Call init_sde(db_sde) first.")
    session = _db_sde.session

    groups = session.query(Groups).filter(
        Groups.published == 1,
        Groups.categoryID == 25
    ).all()
    if not groups:
        return []
    group_ids = [g.id for g in groups]

    type_q = session.query(Types).filter(
        Types.published == 1,
        Types.groupID.in_(group_ids)
    )
    if ore_ids:
        type_q = type_q.filter(Types.id.in_(ore_ids))
    ore_types = type_q.all()
    if not ore_types:
        return []
    type_ids = [t.id for t in ore_types]

    # Detect denormalized JSON materials (column 'materials') vs normalized
    sample = session.query(TypeMaterials).filter(TypeMaterials.id.in_(type_ids)).first()
    json_mode = bool(sample and getattr(sample, "materials", None))

    mats_by_type: Dict[int, List[Dict[str, Any]]] = {}
    material_ids = set()

    if json_mode:
        rows = session.query(TypeMaterials).filter(TypeMaterials.id.in_(type_ids)).all()
        for r in rows:
            raw = r.materials
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = []
            elif isinstance(raw, list):
                parsed = raw
            else:
                parsed = []
            for entry in parsed:
                mid = entry.get("materialTypeID")
                qty = entry.get("quantity")
                if mid is None or qty is None:
                    continue
                material_ids.add(mid)
                mats_by_type.setdefault(r.id, []).append({
                    "materialTypeID": mid,
                    "quantity": qty
                })
    else:
        rows = session.query(TypeMaterials).filter(TypeMaterials.id.in_(type_ids)).all()
        for r in rows:
            mid = r.materialTypeID
            qty = r.quantity
            material_ids.add(mid)
            mats_by_type.setdefault(r.id, []).append({
                "materialTypeID": mid,
                "quantity": qty
            })

    mat_name_map = _resolve_type_names(session, list(material_ids), lang=lang)

    # Attach materialName
    for tid, mats in mats_by_type.items():
        for m in mats:
            m["materialName"] = mat_name_map.get(m["materialTypeID"], str(m["materialTypeID"]))

    ores = []
    for t in ore_types:
        portion = getattr(t, "portionSize", None) or 100
        ores.append({
            "id": t.id,
            "name": _parse_localized(t.name, lang) or str(t.id),
            "ore_price": 0.0,
            "portionSize": portion,
            "volume": getattr(t, "volume", 0.1) or 0.1,  # ADD volume so UI can show ore m3
            "materials": mats_by_type.get(t.id, [])
        })
    return ores

# -------- minerals --------
def get_mineral_list(lang: str = "en"):
    """
    Returns list of base minerals (groupID=18) with metadata.
    """
    if _db_sde is None:
        raise RuntimeError("SDE DB not initialized. Call init_sde(db_sde) first.")
    session = _db_sde.session

    # Query full Types rows (need name etc.)
    mineral_rows = session.query(Types).filter(
        Types.published == 1,
        Types.groupID == 18
    ).all()

    out = []
    for t in mineral_rows:
        out.append({
            "id": t.id,
            "name": _parse_localized(t.name, lang) or str(t.id),
            "volume": getattr(t, "volume", 0.01),
            "basePrice": getattr(t, "basePrice", 0.0)
        })
    out.sort(key=lambda r: r["name"])
    return out