"""The ore calculator's reprocessing facility: one, configured in the admin
settings (reprocessing/facility_type, rig, security_band, tax), with its
values from the SDE's dogma attributes (domain.reprocessing has the formula):

- an Athanor's or Tatara's own bonus: attribute 2722 strRefiningYieldBonus;
- a rig's base yield: attribute 717 refiningYieldMultiplier (0.51 T1, 0.53 T2),
  and its security modifier: 2355/2356/2357 hiSec/lowSec/nullSecModifier;
- an unrigged structure's base yield: attribute 717 of the Standup
  Reprocessing Facility I service module;
- an NPC station's base yield: domain.reprocessing.NPC_STATION_BASE_YIELD (the
  SDE has no station type attribute for it).

An invalid setting raises ReprocessingSettingsInvalid naming it; a missing
attribute raises SdeIndustryDataMissing with the re-import hint. Neither falls
back to a default.
"""
from __future__ import annotations

from typing import Any

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
_FACILITY_NAMES = {_r.FACILITY_NPC_STATION: "NPC station", _r.FACILITY_ATHANOR: "Athanor",
                   _r.FACILITY_TATARA: "Tatara"}
_RIG_NAMES = {_r.RIG_NONE: "no rig", _r.RIG_T1: "T1 rig", _r.RIG_T2: "T2 rig"}
_SECURITY_NAMES = {_r.SECURITY_HIGH: "highsec", _r.SECURITY_LOW: "lowsec", _r.SECURITY_NULL: "nullsec/wormhole"}


class ReprocessingSettingsInvalid(ServiceError):
    """An admin setting of the reprocessing facility is not a valid value."""

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


def facility_from_choice(*, facility_type: Any, rig: Any, security_band: Any, tax: Any,
                         data: _ore.OreData) -> _r.ReprocessingFacility:
    """The facility for these setting values, from the SDE attributes in `data`."""
    facility_type = _check("facility_type", facility_type, _r.FACILITY_TYPES)
    rig = _check("rig", rig, _r.RIGS)
    security_band = _check("security_band", security_band, _r.SECURITY_BANDS)
    if not isinstance(tax, (int, float)) or isinstance(tax, bool) or not 0.0 <= float(tax) <= 1.0:
        raise ReprocessingSettingsInvalid(f"admin setting {SETTINGS_CATEGORY}/tax={tax!r} is not a fraction 0-1")
    if facility_type == _r.FACILITY_NPC_STATION and rig != _r.RIG_NONE:
        raise ReprocessingSettingsInvalid(f"admin setting {SETTINGS_CATEGORY}/rig={rig!r}: an NPC station has no rigs")

    if facility_type == _r.FACILITY_NPC_STATION:
        base, security, structure_bonus = _r.NPC_STATION_BASE_YIELD, 1.0, 0.0
    else:
        structure = _STRUCTURE_TYPE_IDS[facility_type]
        structure_bonus = _attribute(data, structure, _ore.STR_REFINING_YIELD_BONUS_ATTRIBUTE_ID) / 100.0
        if rig == _r.RIG_NONE:
            base = _attribute(data, _ore.STANDUP_REPROCESSING_FACILITY_TYPE_ID,
                              _ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID)
            security = 1.0
        else:
            rig_type = _ore.RIG_TYPE_IDS[(structure, rig)]
            base = _attribute(data, rig_type, _ore.REFINING_YIELD_MULTIPLIER_ATTRIBUTE_ID)
            security = _attribute(data, rig_type, _SECURITY_ATTRIBUTE_IDS[security_band])
    name = f"{_FACILITY_NAMES[facility_type]}, {_RIG_NAMES[rig]}"
    if rig != _r.RIG_NONE:
        name += f", {_SECURITY_NAMES[security_band]}"
    name += f", {float(tax) * 100:g}% tax (admin settings)"
    return _r.ReprocessingFacility(name=name, base_yield=base, security_multiplier=security,
                                   structure_bonus=structure_bonus, tax=float(tax))


def configured_facility(admin_settings: Any, data: _ore.OreData | None = None) -> _r.ReprocessingFacility:
    """The facility the admin settings configure (data: the cached OreData)."""
    if admin_settings is None:
        raise ReprocessingSettingsInvalid("admin settings not loaded")

    def get(key: str) -> Any:
        return admin_settings.get(SETTINGS_CATEGORY, key)

    return facility_from_choice(facility_type=get("facility_type"), rig=get("rig"),
                                security_band=get("security_band"), tax=get("tax"),
                                data=data if data is not None else _ore.ore_data())


def get_facility(admin_settings: Any) -> dict:
    facility = configured_facility(admin_settings)
    return {
        "id": CONFIGURED_FACILITY_ID,
        "name": facility.name,
        "facility_type": admin_settings.get(SETTINGS_CATEGORY, "facility_type"),
        "rig": admin_settings.get(SETTINGS_CATEGORY, "rig"),
        "security_band": admin_settings.get(SETTINGS_CATEGORY, "security_band"),
        "base_yield": facility.base_yield,
        "security_multiplier": facility.security_multiplier,
        "structure_bonus": facility.structure_bonus,
        "multiplier": facility.multiplier,
        "tax": facility.tax,
    }


def get_all_facilities(admin_settings: Any) -> list[dict]:
    return [get_facility(admin_settings)]
