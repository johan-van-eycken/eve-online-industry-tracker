"""The ore calculator's reprocessing facility: one, configured in the admin
settings (reprocessing/facility_type, rig, athanor_rig_asteroid/_moon/_ice,
security_band, npc_station_id, tax), with its values from the SDE
(domain.reprocessing has the formula):

- an NPC station's base yield and base tax: npcStations.reprocessingEfficiency
  and reprocessingStationsTake of the station npc_station_id names, for
  every ore class;
- an Athanor's or Tatara's own bonus: typeDogma attribute 2722 strRefiningYieldBonus;
- the rigs, per ore class (asteroid ore, moon ore, ice): an Athanor has an
  M-Set rig per class (athanor_rig_*), a Tatara the one L-Set Reprocessing
  Monitor (rig). The classes a rig covers come from its dogma effects
  (ore.rig_ore_classes); a covered class gets the rig's attribute 717
  refiningYieldMultiplier (0.51 T1, 0.53 T2) and its security modifier
  2355/2356/2357 hiSec/lowSec/nullSecModifier;
- a class without a rig: the Standup Reprocessing Facility I service's
  attribute for the class (2444/2445/2448, 0.50) and no security modifier.

A stored single `rig` on an Athanor from before the per-class rigs is read
as the asteroid rig (config.admin_settings migrates it at load).

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
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingInvalid
from eve_online_industry_tracker.domain import reprocessing as _r
from eve_online_industry_tracker.infrastructure.sde import locations as _locations
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
_RIG_SHORT = {_r.RIG_NONE: "none", _r.RIG_T1: "T1", _r.RIG_T2: "T2"}
_SECURITY_NAMES = {_r.SECURITY_HIGH: "highsec", _r.SECURITY_LOW: "lowsec", _r.SECURITY_NULL: "nullsec/wormhole"}
_STATION_COLUMNS = ("reprocessingEfficiency", "reprocessingStationsTake")

NpcStationLookup = Callable[[int], "tuple[float, float] | None"]
StationNameLookup = Callable[[int], "tuple[str | None, str | None]"]


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
    """The tax setting, within its schema bounds (a stored value outside them
    is kept by the settings manager so that this fails)."""
    spec = ADMIN_SETTINGS_SCHEMA[SETTINGS_CATEGORY]["settings"]["tax"]
    low, high = float(spec["min"]), float(spec["max"])
    if not isinstance(tax, (int, float)) or isinstance(tax, bool) or not low <= float(tax) <= high:
        raise ReprocessingSettingsInvalid(
            f"admin setting {SETTINGS_CATEGORY}/tax={tax!r} is not a fraction {low:g}-{high:g}")
    return float(tax)


def _station_label(station_id: int, station_name: StationNameLookup | None) -> str:
    """"<name> (<id>)", or the id and why there is no name."""
    if station_name is None:
        return f"NPC station {station_id}"
    name, problem = station_name(station_id)
    if name:
        return f"{name} ({station_id})"
    return f"NPC station {station_id} (name unknown: {problem})"


def npc_station_name(session: Any, station_id: int, language: str = "en") -> tuple[str | None, str | None]:
    """The NPC station's full name (locations.npc_station_name), or (None, reason)."""
    if session is None:
        return None, "no SDE session"
    return _locations.npc_station_name(session, station_id, language)


def _check_rigs(athanor_rigs: Any) -> dict[str, str]:
    rigs = athanor_rigs if isinstance(athanor_rigs, dict) else {}
    return {cls: _check(f"athanor_rig_{cls}", rigs.get(cls), _r.RIGS) for cls in _r.ORE_CLASSES}


def _rigged(data: _ore.OreData, rig_type: int, ore_class: str, security_band: str, tier: str) -> _r.ClassYield:
    if ore_class not in data.rig_ore_classes.get(rig_type, frozenset()):
        raise SdeIndustryDataMissing(f"SDE dogma of rig {rig_type} does not give the {ore_class} ore class a "
                                     f"rigged yield (its typeDogma effects / dogmaEffects modifiers); {REIMPORT_HINT}")
    return _r.ClassYield(base_yield=_attribute(data, rig_type, _ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID),
                         security_multiplier=_attribute(data, rig_type, _SECURITY_ATTRIBUTE_IDS[security_band]),
                         rig=tier)


def _unrigged(data: _ore.OreData, ore_class: str) -> _r.ClassYield:
    return _r.ClassYield(base_yield=_attribute(data, _ore.STANDUP_REPROCESSING_FACILITY_TYPE_ID,
                                               _ore.ORE_CLASS_YIELD_ATTRIBUTE_IDS[ore_class]),
                         security_multiplier=1.0, rig=_r.RIG_NONE)


def facility_from_choice(*, facility_type: Any, rig: Any, security_band: Any, tax: Any, npc_station_id: Any,
                         npc_station: NpcStationLookup, data: _ore.OreData,
                         athanor_rigs: Any = None, station_name: StationNameLookup | None = None,
                         ) -> _r.ReprocessingFacility:
    """The facility for these setting values. `rig` is a Tatara's L-Set
    Reprocessing Monitor (none at an NPC station; not read for an Athanor);
    athanor_rigs an Athanor's {ore class: rig}, each required. npc_station(id)
    gives an NPC station's (base yield, base tax) or None; station_name(id)
    its name or (None, reason); `data` has the SDE attributes."""
    facility_type = _check("facility_type", facility_type, _r.FACILITY_TYPES)
    security_band = _check("security_band", security_band, _r.SECURITY_BANDS)

    if facility_type == _r.FACILITY_NPC_STATION:
        rig = _check("rig", rig, _r.RIGS)
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
            name=f"{_station_label(int(npc_station_id), station_name)}, {tax_value * 100:g}% {tax_label} "
                 "(admin settings)",
            yields={cls: _r.ClassYield(base_yield=base, security_multiplier=1.0) for cls in _r.ORE_CLASSES},
            structure_bonus=0.0, tax=tax_value, tax_source=tax_source)

    if facility_type == _r.FACILITY_ATHANOR:
        rigs = _check_rigs(athanor_rigs)
    else:
        rig = _check("rig", rig, _r.RIGS)
        rigs = {cls: rig for cls in _r.ORE_CLASSES}
    if tax is None:
        raise ReprocessingSettingsInvalid(
            f"admin setting {SETTINGS_CATEGORY}/tax is not set: a player structure's tax is the owner's")
    tax_value = _tax(tax)
    structure = _STRUCTURE_TYPE_IDS[facility_type]
    structure_bonus = _attribute(data, structure, _ore.STR_REFINING_YIELD_BONUS_ATTRIBUTE_ID) / 100.0

    yields: dict[str, _r.ClassYield] = {}
    for cls in _r.ORE_CLASSES:
        tier = rigs[cls]
        if facility_type == _r.FACILITY_TATARA and tier != _r.RIG_NONE:
            rig_type = _ore.TATARA_RIG_TYPE_IDS[tier]
            # The monitor's dogma decides which classes it covers; any other gets the unrigged base.
            covered = cls in data.rig_ore_classes.get(rig_type, frozenset())
            yields[cls] = _rigged(data, rig_type, cls, security_band, tier) if covered else _unrigged(data, cls)
        elif tier != _r.RIG_NONE:
            yields[cls] = _rigged(data, _ore.ATHANOR_RIG_TYPE_IDS[(cls, tier)], cls, security_band, tier)
        else:
            yields[cls] = _unrigged(data, cls)

    if facility_type == _r.FACILITY_ATHANOR:
        name = (f"Athanor, rigs: " + ", ".join(f"{cls} {_RIG_SHORT[rigs[cls]]}" for cls in _r.ORE_CLASSES))
    else:
        name = f"{_FACILITY_NAMES[facility_type]}, {_RIG_NAMES[rigs[_r.ORE_CLASS_ASTEROID]]}"
    if any(y.rig != _r.RIG_NONE for y in yields.values()):
        name += f", {_SECURITY_NAMES[security_band]}"
    name += f", {tax_value * 100:g}% tax (admin settings)"
    return _r.ReprocessingFacility(name=name, yields=yields, structure_bonus=structure_bonus, tax=tax_value,
                                   tax_source=_r.TAX_SETTING)


def _settings_choice(admin_settings: Any) -> dict[str, Any]:
    """The reprocessing settings, read as the facility needs them."""
    if admin_settings is None:
        raise ReprocessingSettingsInvalid("admin settings not loaded")

    def get(key: str) -> Any:
        try:
            return admin_settings.get(SETTINGS_CATEGORY, key)
        except AdminSettingInvalid as exc:   # a stored invalid value
            raise ReprocessingSettingsInvalid(
                f"admin setting {exc.category}/{exc.key}={exc.value!r} is invalid ({exc.problem})") from exc

    return {"facility_type": get("facility_type"), "rig": get("rig"), "security_band": get("security_band"),
            "tax": get("tax"), "npc_station_id": get("npc_station_id"),
            "athanor_rigs": {cls: get(f"athanor_rig_{cls}") for cls in _r.ORE_CLASSES}}


def configured_facility(admin_settings: Any, sde_session: Any, data: _ore.OreData | None = None,
                        language: str = "en") -> _r.ReprocessingFacility:
    """The facility the admin settings configure (data: the cached OreData;
    language: the NPC station's name)."""
    return facility_from_choice(**_settings_choice(admin_settings),
                                npc_station=lambda station_id: npc_station_reprocessing(sde_session, station_id),
                                station_name=lambda station_id: npc_station_name(sde_session, station_id, language),
                                data=data if data is not None else _ore.ore_data())


def get_facility(admin_settings: Any, sde_session: Any, data: _ore.OreData | None = None,
                 language: str = "en") -> dict:
    choice = _settings_choice(admin_settings)
    facility = configured_facility(admin_settings, sde_session, data, language)
    station_id = choice["npc_station_id"]
    station_name, station_problem = (npc_station_name(sde_session, int(station_id), language)
                                     if station_id is not None else (None, None))
    return {
        "id": CONFIGURED_FACILITY_ID,
        "name": facility.name,
        "facility_type": choice["facility_type"],
        "rig": choice["rig"],
        "athanor_rigs": choice["athanor_rigs"],
        "security_band": choice["security_band"],
        "npc_station_id": station_id,
        "npc_station_name": station_name,
        "npc_station_name_problem": station_problem,
        "structure_bonus": facility.structure_bonus,
        "by_ore_class": {cls: {"rig": y.rig, "base_yield": y.base_yield, "security_multiplier": y.security_multiplier,
                               "multiplier": facility.multiplier_for(cls)}
                         for cls, y in facility.yields.items()},
        "tax": facility.tax,
        "tax_source": facility.tax_source,
    }


def get_all_facilities(admin_settings: Any, sde_session: Any, data: _ore.OreData | None = None,
                       language: str = "en") -> list[dict]:
    return [get_facility(admin_settings, sde_session, data, language)]
