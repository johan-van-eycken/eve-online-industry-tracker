from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from eve_online_industry_tracker.db_models import (
    Factions,
    MapConstellations,
    MapRegions,
    MapSolarSystems,
    NpcCorporations,
    NpcStations,
    StationOperations,
    StationServices,
)

from eve_online_industry_tracker.infrastructure.sde.localization import parse_localized


def get_solar_systems(session: Any, language: str) -> list[dict]:
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

    solar_systems: list[dict] = []

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


def get_npc_stations(session: Any, language: str, system_id: int) -> list[dict]:
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

    stations: list[dict] = []

    for st in stations_q:
        corporation = corporation_map.get(st.ownerID)
        owner_name = parse_localized(getattr(corporation, "name", None), language) if corporation else ""
        operation = operation_map.get(st.operationID)
        station_name = _owner_operation_name(
            owner_name, bool(st.useOperationName and st.operationID),
            parse_localized(getattr(operation, "operationName", None), language) if operation else "")

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
                "owner_name": owner_name,
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


def _owner_operation_name(owner_name: str, use_operation_name: bool, operation_name: str) -> str:
    """The owner and operation part of an NPC station's name ("Caldari Navy
    Assembly Plant"): the owner corporation, then the operation when the
    station uses its operation's name."""
    if use_operation_name and operation_name:
        return f"{owner_name} {operation_name}"
    return owner_name


_ROMAN = ((10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"))


def _roman(number: int) -> str:
    out = ""
    for value, numeral in _ROMAN:
        while number >= value:
            out += numeral
            number -= value
    return out


def npc_station_name(session: Any, station_id: int, language: str) -> tuple[str | None, str | None]:
    """(name, None) for an NPC station, composed as EVE names it from the
    SDE: "<system> <planet in roman numerals>[ - Moon <n>] - <owner>[
    <operation>]", e.g. "Jita IV - Moon 4 - Caldari Navy Assembly Plant";
    (None, reason) when the SDE does not have what the name needs. Never
    raises.

    The SDE tables this app imports have no planet or moon names, so a
    planet or moon with a name of its own (e.g. "Amarr VIII (Oris)", 43
    planets and 137 moons in build 3607705, in the non-imported
    mapPlanets/mapMoons uniqueName) shows by its index instead."""
    try:
        station = session.execute(text(
            'SELECT "solarSystemID", "celestialIndex", "orbitIndex", "ownerID", "operationID", "useOperationName" '
            'FROM "npcStations" WHERE id = :id'), {"id": int(station_id)}).first()
        if station is None:
            return None, f"no NPC station {station_id} in the SDE npcStations"
        system_id, celestial_index, orbit_index, owner_id, operation_id, use_operation_name = station
        system = session.execute(text('SELECT name FROM "mapSolarSystems" WHERE id = :id'),
                                 {"id": system_id}).first()
        if system is None or not parse_localized(system[0], language):
            return None, f"no solar system {system_id} (of NPC station {station_id}) in the SDE mapSolarSystems"
        owner = session.execute(text('SELECT name FROM "npcCorporations" WHERE id = :id'), {"id": owner_id}).first()
        if owner is None or not parse_localized(owner[0], language):
            return None, f"no corporation {owner_id} (the owner of NPC station {station_id}) in the SDE npcCorporations"
        operation_name = ""
        if use_operation_name and operation_id is not None:
            operation = session.execute(text('SELECT "operationName" FROM "stationOperations" WHERE id = :id'),
                                        {"id": operation_id}).first()
            if operation is None:
                return None, (f"no station operation {operation_id} (of NPC station {station_id}) in the SDE "
                              "stationOperations")
            operation_name = parse_localized(operation[0], language)
        if celestial_index is None:
            return None, f"NPC station {station_id} has no celestialIndex in the SDE npcStations"
    except SQLAlchemyError as exc:
        session.rollback()
        return None, f"the SDE station tables could not be read ({type(exc).__name__}: {exc.args[0] if exc.args else exc})"

    location = f"{parse_localized(system[0], language)} {_roman(int(celestial_index))}"
    if orbit_index is not None:
        location += f" - Moon {int(orbit_index)}"
    return (f"{location} - "
            f"{_owner_operation_name(parse_localized(owner[0], language), bool(use_operation_name), operation_name)}"), None
