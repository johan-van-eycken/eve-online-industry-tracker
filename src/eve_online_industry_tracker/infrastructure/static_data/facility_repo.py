"""The ore calculator's reprocessing facility: one, configured in the admin
settings (reprocessing/facility_type, rig, security_band, npc_station_id,
tax), with its values from the SDE (domain.reprocessing has the formula):

- an NPC station's base yield and base tax: npcStations.reprocessingEfficiency
  and reprocessingStationsTake of the station npc_station_id names;
- an Athanor's or Tatara's own bonus: typeDogma attribute 2722 strRefiningYieldBonus;
- a rig's base yield: attribute 717 refiningYieldMultiplier (0.51 T1, 0.53 T2),
  and its security modifier: 2355/2356/2357 hiSec/lowSec/nullSecModifier;
- an unrigged structure's base yield: attribute 717 of the Standup
  Reprocessing Facility I service module.

The tax: at an NPC station the SDE's base tax (before standings), unless
reprocessing/tax is set, which is then the rate after standings; at a player
structure reprocessing/tax is required (the owner sets it; the SDE has none).

An invalid or missing setting raises ReprocessingSettingsInvalid naming it; a
missing SDE value raises SdeIndustryDataMissing with the re-import hint.
Nothing falls back to a constant.
"""
from __future__ import annotations

from typing import Any, Callable

from sqlalchemy import inspect, text

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.domain import reprocessing as _r
from eve_online_industry_tracker.infrastructure.sde import ore as _ore
from eve_online_industry_tracker.infrastructure.sde.industry import REIMPORT_HINT, SdeIndustryDataMissing

SETTINGS_CATEGORY = "reprocessing"
CONFIGURED_FACILITY_ID = 1

_STRUCTURE_TYPE_IDS = {_r.FACILITY_ATHANOR: _ore.ATHANOR_TYPE_ID, _r.FACILITY_TATARA: _ore.TATARA_TYPE_ID}
_SECURITY_ATTRIBUTE_IDS = {
    _r.SECURITY_HIGH: _ore.HISEC_MODIFIER_ATTRIBUTE_ID,
    _r.SECURITY_LOW: _ore.LOWSEC_MODIFIER_ATTRIBUTE_ID,
    _r.SECURITY_NULL: _ore.NULLSEC_MODIFIER_ATTRIBUTE_ID,
}
_FACILITY_NAMES = {_r.FACILITY_ATHANOR: "Athanor", _r.FACILITY_TATARA: "Tatara"}
_RIG_NAMES = {_r.RIG_NONE: "no rig", _r.RIG_T1: "T1 rig", _r.RIG_T2: "T2 rig"}
_SECURITY_NAMES = {_r.SECURITY_HIGH: "highsec", _r.SECURITY_LOW: "lowsec", _r.SECURITY_NULL: "nullsec/wormhole"}
_STATION_COLUMNS = ("reprocessingEfficiency", "reprocessingStationsTake")

NpcStationLookup = Callable[[int], "tuple[float, float] | None"]


class ReprocessingSettingsInvalid(ServiceError):
    """An admin setting of the reprocessing facility is missing or not a valid value."""

    def __init__(self, message: str):
        super().__init__(f"{message}; fix it on the Admin Settings page (Reprocessing)", status_code=500)


def _check(key: str, value: Any, allowed: tuple[str, ...]) -> str:
    if value not in allowed:
        raise ReprocessingSettingsInvalid(
            f"admin setting {SETTINGS_CATEGORY}/{key}={value!r} is not one of {', '.join(allowed)}")
    return value


def _attribute(data: _ore.OreData, type_id: int, attribute_id: int) -> float:
    value = (data.facility_attributes.get(type_id) or {}).get(attribute_id)
    if value is None:
        raise SdeIndustryDataMissing(f"SDE typeDogma lacks attribute {attribute_id} of type {type_id} (the "
                                     f"reprocessing facility); {REIMPORT_HINT}")
    return float(value)


def npc_station_reprocessing(session: Any, station_id: int) -> tuple[float, float] | None:
    """(reprocessingEfficiency, reprocessingStationsTake) of an NPC station in
    the SDE's npcStations; None for an id it does not have."""
    tables = set(inspect(session.get_bind()).get_table_names())
    columns = ({c["name"] for c in inspect(session.get_bind()).get_columns("npcStations")}
               if "npcStations" in tables else set())
    missing = [c for c in _STATION_COLUMNS if c not in columns]
    if missing:
        raise SdeIndustryDataMissing(f"SDE npcStations lacks {', '.join(missing)} (an NPC station's reprocessing "
                                     f"yield and tax); {REIMPORT_HINT}")
    row = session.execute(text('SELECT "reprocessingEfficiency", "reprocessingStationsTake" FROM "npcStations" '
                               'WHERE id = :id'), {"id": int(station_id)}).first()
    if row is None:
        return None
    if row[0] is None or row[1] is None:
        raise SdeIndustryDataMissing(f"SDE npcStations row {station_id} has no reprocessing yield or tax; "
                                     f"{REIMPORT_HINT}")
    return float(row[0]), float(row[1])


def _tax(tax: Any) -> float:
    if not isinstance(tax, (int, float)) or isinstance(tax, bool) or not 0.0 <= float(tax) <= 1.0:
        raise ReprocessingSettingsInvalid(f"admin setting {SETTINGS_CATEGORY}/tax={tax!r} is not a fraction 0-1")
    return float(tax)


def facility_from_choice(*, facility_type: Any, rig: Any, security_band: Any, tax: Any, npc_station_id: Any,
                         npc_station: NpcStationLookup, data: _ore.OreData) -> _r.ReprocessingFacility:
    """The facility for these setting values. npc_station(id) gives an NPC
    station's (base yield, base tax) or None; `data` has the SDE attributes."""
    facility_type = _check("facility_type", facility_type, _r.FACILITY_TYPES)
    rig = _check("rig", rig, _r.RIGS)
    security_band = _check("security_band", security_band, _r.SECURITY_BANDS)

    if facility_type == _r.FACILITY_NPC_STATION:
        if rig != _r.RIG_NONE:
            raise ReprocessingSettingsInvalid(
                f"admin setting {SETTINGS_CATEGORY}/rig={rig!r}: an NPC station has no rigs")
        if npc_station_id is None:
            raise ReprocessingSettingsInvalid(
                f"admin setting {SETTINGS_CATEGORY}/npc_station_id is not set: an NPC station's yield and tax "
                "come from the SDE station")
        station = npc_station(int(npc_station_id))
        if station is None:
            raise ReprocessingSettingsInvalid(
                f"admin setting {SETTINGS_CATEGORY}/npc_station_id={npc_station_id!r} is not an NPC station in "
                "the SDE")
        base, base_tax = station
        if tax is None:
            tax_value, tax_source, tax_label = base_tax, _r.TAX_SDE_STATION_BASE, "SDE base tax"
        else:
            tax_value, tax_source, tax_label = _tax(tax), _r.TAX_SETTING_AFTER_STANDINGS, "tax after standings"
        return _r.ReprocessingFacility(
            name=f"NPC station {int(npc_station_id)}, {tax_value * 100:g}% {tax_label} (admin settings)",
            base_yield=base, security_multiplier=1.0, structure_bonus=0.0, tax=tax_value, tax_source=tax_source)

    if tax is None:
        raise ReprocessingSettingsInvalid(
            f"admin setting {SETTINGS_CATEGORY}/tax is not set: a player structure's tax is the owner's")
    tax_value = _tax(tax)
    structure = _STRUCTURE_TYPE_IDS[facility_type]
    structure_bonus = _attribute(data, structure, _ore.STR_REFINING_YIELD_BONUS_ATTRIBUTE_ID) / 100.0
    if rig == _r.RIG_NONE:
        base = _attribute(data, _ore.STANDUP_REPROCESSING_FACILITY_TYPE_ID, _ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID)
        security = 1.0
    else:
        rig_type = _ore.RIG_TYPE_IDS[(structure, rig)]
        base = _attribute(data, rig_type, _ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID)
        security = _attribute(data, rig_type, _SECURITY_ATTRIBUTE_IDS[security_band])
    name = f"{_FACILITY_NAMES[facility_type]}, {_RIG_NAMES[rig]}"
    if rig != _r.RIG_NONE:
        name += f", {_SECURITY_NAMES[security_band]}"
    name += f", {tax_value * 100:g}% tax (admin settings)"
    return _r.ReprocessingFacility(name=name, base_yield=base, security_multiplier=security,
                                   structure_bonus=structure_bonus, tax=tax_value, tax_source=_r.TAX_SETTING)


def configured_facility(admin_settings: Any, sde_session: Any,
                        data: _ore.OreData | None = None) -> _r.ReprocessingFacility:
    """The facility the admin settings configure (data: the cached OreData)."""
    if admin_settings is None:
        raise ReprocessingSettingsInvalid("admin settings not loaded")

    def get(key: str) -> Any:
        return admin_settings.get(SETTINGS_CATEGORY, key)

    return facility_from_choice(facility_type=get("facility_type"), rig=get("rig"),
                                security_band=get("security_band"), tax=get("tax"),
                                npc_station_id=get("npc_station_id"),
                                npc_station=lambda station_id: npc_station_reprocessing(sde_session, station_id),
                                data=data if data is not None else _ore.ore_data())


def get_facility(admin_settings: Any, sde_session: Any) -> dict:
    facility = configured_facility(admin_settings, sde_session)
    return {
        "id": CONFIGURED_FACILITY_ID,
        "name": facility.name,
        "facility_type": admin_settings.get(SETTINGS_CATEGORY, "facility_type"),
        "rig": admin_settings.get(SETTINGS_CATEGORY, "rig"),
        "security_band": admin_settings.get(SETTINGS_CATEGORY, "security_band"),
        "npc_station_id": admin_settings.get(SETTINGS_CATEGORY, "npc_station_id"),
        "base_yield": facility.base_yield,
        "security_multiplier": facility.security_multiplier,
        "structure_bonus": facility.structure_bonus,
        "multiplier": facility.multiplier,
        "tax": facility.tax,
        "tax_source": facility.tax_source,
    }


def get_all_facilities(admin_settings: Any, sde_session: Any) -> list[dict]:
    return [get_facility(admin_settings, sde_session)]
