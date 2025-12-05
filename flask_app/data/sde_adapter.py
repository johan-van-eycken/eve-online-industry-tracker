"""
Adapter for retrieving Static Data Export data from the local database.
"""
import json
import re
from typing import Dict, List

from classes.database_models import (
    Blueprints, Categories, Groups, TypeMaterials, Types
    , MapSolarSystems, MapRegions, MapConstellations, Factions, Races
    , NpcStations, NpcCorporations, StationOperations, StationServices
)

_db_sde = None
_language = None


def sde_adapter(db) -> None:
    global _db_sde, _language
    _db_sde = db
    _language = db.language or "en"


def _ensure() -> None:
    if _db_sde is None:
        raise RuntimeError("SDE DB not initialized. Call init_sde(db_sde) first.")
    if _language is None:
        raise RuntimeError("Language not set in SDE adapter.")


# -------- helpers --------
def _parse_localized(raw) -> str:
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
    clean = re.sub(r"<[^>]+>", "", text).replace("\r\n", "<br>").strip()

    return clean


# -------- ores --------
def get_all_ores() -> List[dict]:
    _ensure()

    groups = (_db_sde.session.query(Groups).filter(Groups.published == 1, Groups.categoryID == 25).all())
    if not groups:
        return []
    
    group_ids = [g.id for g in groups]
    type_q = (_db_sde.session.query(Types).filter(Types.published == 1, Types.groupID.in_(group_ids)).all())
    if not type_q:
        return []

    ores = []
    for t in type_q:
        type_group = (_db_sde.session.query(Groups).filter(Groups.id == t.groupID).first())
        type_category = (_db_sde.session.query(Categories).filter(Categories.id == type_group.categoryID).first())
        type_mat_q = (_db_sde.session.query(TypeMaterials).filter(TypeMaterials.id == t.id).all())

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
            "materials": [],
        }
        for tm in type_mat_q:
            for mat in tm.materials:
                mat_type = (_db_sde.session.query(Types).filter(Types.id == mat["materialTypeID"]).first())
                mat_group = (_db_sde.session.query(Groups).filter(Groups.id == mat_type.groupID).first())
                mat_category = (_db_sde.session.query(Categories).filter(Categories.id == mat_group.categoryID).first())

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
                    "quantity": mat["quantity"],
                })

        ores.append(ore)
    return ores


# -------- materials --------
def get_all_materials() -> List[dict]:
    """
    Returns list of base materials (groupID=18) with metadata.
    """
    _ensure()

    # Query full Types rows (need name etc.)
    material_rows = (
        _db_sde.session.query(Types)
        .filter(Types.published == 1, Types.groupID == 18, Types.metaGroupID == None)
        .all()
    )

    out = []
    for t in material_rows:
        out.append(
            {
                "id": t.id,
                "name": _parse_localized(t.name) or str(t.id),
                "volume": getattr(t, "volume", 0.01),
                "basePrice": getattr(t, "basePrice", 0.0),
            }
        )
    out.sort(key=lambda r: r["id"])
    return out


# -------- blueprints --------
def get_blueprint_manufacturing_data() -> Dict[int, Dict]:
    """
    Returns manufacturing materials and products and research times for all blueprints.
    """
    _ensure()

    blueprints = _db_sde.session.query(Blueprints).all()
    if not blueprints:
        return {}

    # Get all blueprint, material, product and skill type IDs for batch lookup
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

    # Batch fetch all type data
    all_type_ids = material_type_ids | product_type_ids | blueprint_type_ids | skill_type_ids
    type_data_map = {t.id: t for t in get_type_data(list(all_type_ids))}

    # Build result
    result = {}
    for bp in blueprints:
        activities = bp.activities if isinstance(bp.activities, dict) else {}
        manufacturing = activities.get("manufacturing", {})

        materials = []
        for mat in manufacturing.get("materials", []):
            type_id = mat.get("typeID", None)
            type_data = type_data_map.get(type_id)
            materials.append(
                {
                    "type_id": type_id,
                    "type_name": type_data.name if type_data else "",
                    "group_id": type_data.group_id if type_data else None,
                    "group_name": type_data.group_name if type_data else "",
                    "category_id": type_data.category_id if type_data else None,
                    "category_name": type_data.category_name if type_data else "",
                    "quantity": mat["quantity"],
                }
            )

        products = []
        for prod in manufacturing.get("products", []):
            type_id = prod.get("typeID", None)
            type_data = type_data_map.get(type_id)
            products.append(
                {
                    "type_id": type_id,
                    "type_name": type_data.name if type_data else "",
                    "group_id": type_data.group_id if type_data else None,
                    "group_name": type_data.group_name if type_data else "",
                    "category_id": type_data.category_id if type_data else None,
                    "category_name": type_data.category_name if type_data else "",
                    "quantity": prod["quantity"],
                }
            )
        
        skills = []
        for skill in manufacturing.get("skills", []):
            type_id = skill.get("typeID", None)
            type_data = type_data_map.get(type_id)
            skills.append(
                {
                    "type_id": type_id,
                    "type_name": type_data.name if type_data else "",
                    "group_id": type_data.group_id if type_data else None,
                    "group_name": type_data.group_name if type_data else "",
                    "category_id": type_data.category_id if type_data else None,
                    "category_name": type_data.category_name if type_data else "",
                    "level": skill["level"],
                }
            )

        type_id = bp.blueprintTypeID
        type_data = type_data_map.get(type_id)
        result[bp.blueprintTypeID] = {
            "type_id": type_id,
            "type_name": type_data.name if type_data else "",
            "type_meta_group_id": type_data.meta_group_id if type_data else None,
            "group_id": type_data.group_id if type_data else None,
            "group_name": type_data.group_name if type_data else "",
            "category_id": type_data.category_id if type_data else None,
            "category_name": type_data.category_name if type_data else "",
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

# -------- solar systems --------
def get_solar_systems() -> List[dict]:
    """
    Returns list of solar systems with metadata.
    """
    _ensure()

    solar_systems_q = _db_sde.session.query(MapSolarSystems).all()
    region_ids = {ss.regionID for ss in solar_systems_q}
    constellation_ids = {ss.constellationID for ss in solar_systems_q}
    regions_q = _db_sde.session.query(MapRegions).filter(MapRegions.id.in_(region_ids)).all()
    region_map = {r.id: r for r in regions_q}
    constellations_q = _db_sde.session.query(MapConstellations).filter(MapConstellations.id.in_(constellation_ids)).all()
    constellation_map = {c.id: c for c in constellations_q}
    faction_ids = {c.factionID for c in constellations_q if c.factionID is not None}
    factions_q = _db_sde.session.query(Factions).filter(Factions.id.in_(faction_ids)).all()
    faction_map = {f.id: f for f in factions_q}

    solar_systems = []
    for ss in solar_systems_q:
        region = region_map.get(ss.regionID)
        solar_systems.append(
            {
                "id": ss.id,
                "name": _parse_localized(ss.name) or str(ss.id),
                "security_status": ss.securityStatus,
                "region_id": ss.regionID,
                "region_name": _parse_localized(region.name) if region else "",
                "region_description": _parse_localized(region.description) if region else "",
                "constellation_id": ss.constellationID,
                "constellation_name": _parse_localized(constellation_map.get(ss.constellationID).name) if constellation_map.get(ss.constellationID) else "",
                "faction_id": constellation_map.get(ss.constellationID).factionID if constellation_map.get(ss.constellationID) else None,
                "faction_name": _parse_localized(faction_map.get(constellation_map.get(ss.constellationID).factionID).name) if constellation_map.get(ss.constellationID) and faction_map.get(constellation_map.get(ss.constellationID).factionID) else "",
            }
        )
    return solar_systems

# -------- NPC stations --------
def get_npc_stations(system_id: int) -> List[dict]:
    """
    Returns list of NPC stations with metadata.
    """
    if not system_id:
        raise ValueError("System ID is required to fetch NPC stations.")

    _ensure()

    stations_q = _db_sde.session.query(NpcStations).filter(NpcStations.solarSystemID == system_id).all()
    owner_ids = {st.ownerID for st in stations_q}
    corporations_q = _db_sde.session.query(NpcCorporations).filter(NpcCorporations.id.in_(owner_ids)).all()
    corporation_map = {c.id: c for c in corporations_q}
    operation_ids = {st.operationID for st in stations_q if st.operationID is not None}
    operations_q = _db_sde.session.query(StationOperations).filter(StationOperations.id.in_(operation_ids)).all()
    operation_map = {o.id: o for o in operations_q}
    services_q = _db_sde.session.query(StationServices).all()
    services_map = {s.id: s for s in services_q}

    stations = []
    for st in stations_q:
        corporation = corporation_map.get(st.ownerID)
        station_name = _parse_localized(corporation.name) if corporation else ""
        operation = operation_map.get(st.operationID)

        if st.useOperationName and st.operationID:
            operation_name = _parse_localized(operation.operationName) if operation else ""
            if operation_name:
                station_name += " " + operation_name

        service_ids = operation.services if operation else []
        services = []
        for service_id in service_ids:
            service = services_map.get(service_id)
            if service:
                service_name = _parse_localized(service.serviceName)
                services.append(
                    {
                        "service_id": service_id,
                        "service_name": service_name or "",
                    }
                )

        stations.append(
            {
                "station_id": st.id,
                "station_name": station_name,
                "type_id": st.typeID,
                "system_id": st.solarSystemID,
                "owner_id": st.ownerID,
                "operation_id": st.operationID,
                "reprocessing_efficiency": st.reprocessingEfficiency,
                "reprocessing_hangar_flag": st.reprocessingHangarFlag,
                "reprocessing_stations_take": st.reprocessingStationsTake,
                "services": services,
                "ratio": operation.ratio if operation else None,
                "manufacturing_factor": operation.manufacturingFactor if operation else None,
                "research_factor": operation.researchFactor if operation else None,
            }
        )
    return stations

# -------- type data --------
def get_type_data(type_ids: List[int]) -> Dict[int, Dict]:
    """
    Returns type metadata for the given list of type IDs.
    """
    if not type_ids:
        return {}

    _ensure()

    types_q = _db_sde.session.query(Types).filter(Types.id.in_(type_ids)).all()
    group_ids = {t.groupID for t in types_q if hasattr(t, "groupID")}
    group_data_map = {g.id: g for g in _db_sde.session.query(Groups).filter(Groups.id.in_(group_ids)).all()}
    category_ids = {g.categoryID for g in group_data_map.values() if hasattr(g, "categoryID")}
    category_data_map = {c.id: c for c in _db_sde.session.query(Categories).filter(Categories.id.in_(category_ids)).all()}
    race_ids = {t.raceID for t in types_q if hasattr(t, "raceID") and t.raceID is not None}
    race_data_map = {r.id: r for r in _db_sde.session.query(Races).filter(Races.id.in_(race_ids)).all()}
    faction_ids = {t.factionID for t in types_q if hasattr(t, "factionID") and t.factionID is not None}
    faction_data_map = {f.id: f for f in _db_sde.session.query(Factions).filter(Factions.id.in_(faction_ids)).all()}

    result = {}
    for t in types_q:
        group = group_data_map.get(t.groupID)
        category = category_data_map.get(group.categoryID) if group else None
        race = race_data_map.get(t.raceID) if t.raceID is not None else None
        faction = faction_data_map.get(t.factionID) if t.factionID is not None else None
        result[t.id] = {
            "type_id": t.id,
            "type_name": _parse_localized(t.name) or str(t.id),
            "volume": t.volume,
            "repackaged_volume": t.repackagedVolume,
            "radius": t.radius,
            "portion_size": t.portionSize,
            "description": _parse_localized(t.description),
            "base_price": t.basePrice,
            "icon_id": t.iconID,
            "meta_group_id": t.metaGroupID,
            "group_id": t.groupID,
            "group_name": _parse_localized(group.name) if group else "",
            "group_icon_id": group.iconID if group else None,
            "group_anchorable": group.anchorable if group else None,
            "group_anchored": group.anchored if group else None,
            "group_use_base_price": group.useBasePrice if group else None,
            "group_fittable_non_singleton": group.fittableNonSingleton if group else None,
            "group_repackaged_volume": group.repackagedVolume if group else None,
            "category_id": group.categoryID if group else None,
            "category_name": _parse_localized(category.name) if category else "",
            "category_icon_id": category.iconID if category else None,
            "race_id": t.raceID,
            "race_name": _parse_localized(race.name) if race else "",
            "race_description": _parse_localized(race.description) if race else "",
            "race_icon_id": race.iconID if race else None,
            "race_ship_type_id": race.shipTypeID if race else None,
            "race_skills": race.skills if race else None,
            "faction_id": t.factionID,
            "faction_name": _parse_localized(faction.name) if faction else "",
            "faction_description": _parse_localized(faction.description) if faction else "",
            "faction_short_description": _parse_localized(faction.shortDescription) if faction else "",
            "faction_flat_logo": faction.factionFlatLogo if faction else None,
            "faction_logo_with_name": faction.factionLogoWithName if faction else None,
            "faction_member_races": faction.memberRaces if faction else None,
            "faction_corporation_id": faction.corporationID if faction else None,
            "faction_militia_corporation_id": faction.militiaCorporationID if faction else None,
            "faction_solar_system_id": faction.solarSystemID if faction else None,
        }

    return result