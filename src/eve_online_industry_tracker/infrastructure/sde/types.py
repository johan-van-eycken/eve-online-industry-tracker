from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.db_models import Categories, Factions, Groups, Races, Types

from eve_online_industry_tracker.infrastructure.sde.localization import parse_localized


def get_type_data(session: Any, language: str, type_ids: list[int]) -> dict[int, dict]:
    """Return type metadata for the given list of type IDs."""
    if not type_ids:
        return {}

    types_q = session.query(Types).filter(Types.id.in_(type_ids)).all()
    group_ids = {t.groupID for t in types_q if getattr(t, "groupID", None) is not None}
    group_data_map = {g.id: g for g in session.query(Groups).filter(Groups.id.in_(group_ids)).all()}

    category_ids = {g.categoryID for g in group_data_map.values() if getattr(g, "categoryID", None) is not None}
    category_data_map = {c.id: c for c in session.query(Categories).filter(Categories.id.in_(category_ids)).all()}

    race_ids = {t.raceID for t in types_q if getattr(t, "raceID", None) is not None}
    race_data_map = {r.id: r for r in session.query(Races).filter(Races.id.in_(race_ids)).all()}

    faction_ids = {t.factionID for t in types_q if getattr(t, "factionID", None) is not None}
    faction_data_map = {f.id: f for f in session.query(Factions).filter(Factions.id.in_(faction_ids)).all()}

    result: dict[int, dict] = {}

    for t in types_q:
        group = group_data_map.get(t.groupID)
        category = category_data_map.get(group.categoryID) if group else None
        race = race_data_map.get(t.raceID) if getattr(t, "raceID", None) is not None else None
        faction = faction_data_map.get(t.factionID) if getattr(t, "factionID", None) is not None else None

        result[t.id] = {
            "type_id": t.id,
            "type_name": parse_localized(t.name, language) or str(t.id),
            "volume": getattr(t, "volume", None),
            "repackaged_volume": getattr(t, "repackagedVolume", None),
            "radius": getattr(t, "radius", None),
            "portion_size": getattr(t, "portionSize", None),
            "description": parse_localized(getattr(t, "description", None), language),
            "base_price": getattr(t, "basePrice", None),
            "icon_id": getattr(t, "iconID", None),
            "meta_group_id": getattr(t, "metaGroupID", None),
            "group_id": getattr(t, "groupID", None),
            "group_name": parse_localized(getattr(group, "name", None), language) if group else "",
            "group_icon_id": getattr(group, "iconID", None) if group else None,
            "group_anchorable": getattr(group, "anchorable", None) if group else None,
            "group_anchored": getattr(group, "anchored", None) if group else None,
            "group_use_base_price": getattr(group, "useBasePrice", None) if group else None,
            "group_fittable_non_singleton": getattr(group, "fittableNonSingleton", None) if group else None,
            "group_repackaged_volume": getattr(group, "repackagedVolume", None) if group else None,
            "category_id": getattr(group, "categoryID", None) if group else None,
            "category_name": parse_localized(getattr(category, "name", None), language) if category else "",
            "category_icon_id": getattr(category, "iconID", None) if category else None,
            "race_id": getattr(t, "raceID", None),
            "race_name": parse_localized(getattr(race, "name", None), language) if race else "",
            "race_description": parse_localized(getattr(race, "description", None), language) if race else "",
            "race_icon_id": getattr(race, "iconID", None) if race else None,
            "race_ship_type_id": getattr(race, "shipTypeID", None) if race else None,
            "race_skills": getattr(race, "skills", None) if race else None,
            "faction_id": getattr(t, "factionID", None),
            "faction_name": parse_localized(getattr(faction, "name", None), language) if faction else "",
            "faction_description": parse_localized(getattr(faction, "description", None), language) if faction else "",
            "faction_short_description": parse_localized(getattr(faction, "shortDescription", None), language) if faction else "",
            "faction_flat_logo": getattr(faction, "factionFlatLogo", None) if faction else None,
            "faction_logo_with_name": getattr(faction, "factionLogoWithName", None) if faction else None,
            "faction_member_races": getattr(faction, "memberRaces", None) if faction else None,
            "faction_corporation_id": getattr(faction, "corporationID", None) if faction else None,
            "faction_militia_corporation_id": getattr(faction, "militiaCorporationID", None) if faction else None,
            "faction_solar_system_id": getattr(faction, "solarSystemID", None) if faction else None,
        }

    return result
