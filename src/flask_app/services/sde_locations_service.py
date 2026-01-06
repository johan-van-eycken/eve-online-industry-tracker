from __future__ import annotations

from typing import List

from classes.database_models import (
    Factions,
    MapConstellations,
    MapRegions,
    MapSolarSystems,
    NpcCorporations,
    NpcStations,
    StationOperations,
    StationServices,
)

from flask_app.services.sde_localization import parse_localized


def get_solar_systems(session, language: str) -> List[dict]:
    solar_systems_q = session.query(MapSolarSystems).all()

    region_ids = {ss.regionID for ss in solar_systems_q}
    constellation_ids = {ss.constellationID for ss in solar_systems_q}

    regions_q = session.query(MapRegions).filter(MapRegions.id.in_(region_ids)).all()
    region_map = {r.id: r for r in regions_q}

    constellations_q = session.query(MapConstellations).filter(MapConstellations.id.in_(constellation_ids)).all()
    constellation_map = {c.id: c for c in constellations_q}

    faction_ids = {c.factionID for c in constellations_q if c.factionID is not None}
    factions_q = session.query(Factions).filter(Factions.id.in_(faction_ids)).all()
    faction_map = {f.id: f for f in factions_q}

    solar_systems: List[dict] = []

    for ss in solar_systems_q:
        region = region_map.get(ss.regionID)
        constellation = constellation_map.get(ss.constellationID)
        faction = faction_map.get(constellation.factionID) if constellation and constellation.factionID else None

        solar_systems.append(
            {
                "id": ss.id,
                "name": parse_localized(ss.name, language) or str(ss.id),
                "security_status": ss.securityStatus,
                "region_id": ss.regionID,
                "region_name": parse_localized(getattr(region, "name", None), language) if region else "",
                "region_description": parse_localized(getattr(region, "description", None), language) if region else "",
                "constellation_id": ss.constellationID,
                "constellation_name": parse_localized(getattr(constellation, "name", None), language) if constellation else "",
                "faction_id": getattr(constellation, "factionID", None) if constellation else None,
                "faction_name": parse_localized(getattr(faction, "name", None), language) if faction else "",
            }
        )

    return solar_systems


def get_npc_stations(session, language: str, system_id: int) -> List[dict]:
    if not system_id:
        raise ValueError("System ID is required to fetch NPC stations.")

    stations_q = session.query(NpcStations).filter(NpcStations.solarSystemID == system_id).all()
    owner_ids = {st.ownerID for st in stations_q}

    corporations_q = session.query(NpcCorporations).filter(NpcCorporations.id.in_(owner_ids)).all()
    corporation_map = {c.id: c for c in corporations_q}

    operation_ids = {st.operationID for st in stations_q if st.operationID is not None}
    operations_q = session.query(StationOperations).filter(StationOperations.id.in_(operation_ids)).all()
    operation_map = {o.id: o for o in operations_q}

    services_q = session.query(StationServices).all()
    services_map = {s.id: s for s in services_q}

    stations: List[dict] = []

    for st in stations_q:
        corporation = corporation_map.get(st.ownerID)
        station_name = parse_localized(getattr(corporation, "name", None), language) if corporation else ""
        operation = operation_map.get(st.operationID)

        if st.useOperationName and st.operationID:
            operation_name = parse_localized(getattr(operation, "operationName", None), language) if operation else ""
            if operation_name:
                station_name += " " + operation_name

        service_ids = getattr(operation, "services", None) if operation else []
        services = []
        for service_id in service_ids or []:
            service = services_map.get(service_id)
            if service:
                service_name = parse_localized(getattr(service, "serviceName", None), language)
                services.append({"service_id": service_id, "service_name": service_name or ""})

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
                "ratio": getattr(operation, "ratio", None) if operation else None,
                "manufacturing_factor": getattr(operation, "manufacturingFactor", None) if operation else None,
                "research_factor": getattr(operation, "researchFactor", None) if operation else None,
            }
        )

    return stations
