"""Which industry service modules an Upwell structure can fit, from the SDE.

A structure manufactures a capital ship only with Standup Capital Shipyard I
fitted, a Titan or Supercarrier only with Standup Supercapital Shipyard I.
Each service module lists the structures it fits as SDE dogma attributes:
canFitShipType1..N (structure type ids) and canFitShipGroup01..N (structure
group ids), plus where it may be onlined: disallowInHighSec,
disallowInEmpireSpace (high and low) and requiresSovHubUpgrade (a sovereignty
hub upgrade, so sovereign nullsec only). infrastructure/sde/structure_services
reads them; this module applies them to one structure and band.

At the time of writing the SDE says: the Manufacturing Plant fits every
Citadel, Engineering Complex and Refinery; the Capital Shipyard fits the
Sotiyo, Azbel, Fortizar, Keepstar, the Upwell Palatine Keepstar and the
faction Fortizars, outside highsec; the Supercapital Shipyard fits only the
Sotiyo, outside empire space, with a sovereignty hub upgrade.

Which hulls need a shipyard is CCP's assembly-line data (below).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from eve_online_industry_tracker.domain import security_band as _security

MANUFACTURING_PLANT = "manufacturing_plant"
CAPITAL_SHIPYARD = "capital_shipyard"
SUPERCAPITAL_SHIPYARD = "supercapital_shipyard"
REACTOR = "reactor"

# Service module type ids (SDE group 1415 Structure Engineering Service
# Module, and 1322 Structure Resource Processing Service Module for the
# reactors).
SERVICE_MODULE_TYPE_IDS: dict[str, tuple[int, ...]] = {
    MANUFACTURING_PLANT: (35878,),
    CAPITAL_SHIPYARD: (35881,),
    SUPERCAPITAL_SHIPYARD: (35877,),
    REACTOR: (45537, 45538, 45539),
}


def _ints(values: Any) -> frozenset[int]:
    out: set[int] = set()
    for v in values or ():
        try:
            out.add(int(v))
        except (TypeError, ValueError):
            continue
    return frozenset(out)


def structure_service_fits(
    structure_type_id: int, structure_group_id: int | None, module_fits: dict[int, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """{service: {"fits", "disallow_in_high_sec", "disallow_in_empire_space",
    "requires_sov_upgrade"}} for one structure, from the service modules'
    SDE fitting data (infrastructure/sde/structure_services). A service none
    of whose modules is in the SDE is left out (unknown)."""
    out: dict[str, dict[str, Any]] = {}
    for service, module_type_ids in SERVICE_MODULE_TYPE_IDS.items():
        modules = [module_fits[t] for t in module_type_ids if isinstance(module_fits.get(t), dict)]
        if not modules:
            continue
        fitting = [
            m for m in modules
            if int(structure_type_id) in _ints(m.get("structure_type_ids"))
            or (structure_group_id is not None and int(structure_group_id) in _ints(m.get("structure_group_ids")))
        ]
        out[service] = {
            "fits": bool(fitting),
            "disallow_in_high_sec": any(bool(m.get("disallow_in_high_sec")) for m in fitting or modules),
            "disallow_in_empire_space": any(bool(m.get("disallow_in_empire_space")) for m in fitting or modules),
            "requires_sov_upgrade": any(bool(m.get("requires_sov_upgrade")) for m in fitting or modules),
        }
    return out


SERVICE_LABELS: dict[str, str] = {
    MANUFACTURING_PLANT: "Standup Manufacturing Plant I",
    CAPITAL_SHIPYARD: "Standup Capital Shipyard I",
    SUPERCAPITAL_SHIPYARD: "Standup Supercapital Shipyard I",
    REACTOR: "a Standup reactor",
}

# CCP's assembly lines (Hoboleaks industryassemblylines /
# industryinstallationtypes, revision 3619879; snapshot and diff in
# tests/fixtures/hoboleaks/ and tests/test_facility_gate_and_reactions.py).
# Line 175 "Structure Basic Manufacturing" (Manufacturing Plant) builds every
# other manufacturable hull, Freighters and Jump Freighters included; the
# NPC station line does the same.
SERVICE_ASSEMBLY_LINES: dict[str, tuple[int, ...]] = {
    MANUFACTURING_PLANT: (175,),
    CAPITAL_SHIPYARD: (176,),
    SUPERCAPITAL_SHIPYARD: (176, 177),
}
# Line 176 "Structure Capital Shipyard": Dreadnought, Carrier, Capital
# Industrial Ship, Force Auxiliary, Lancer Dreadnought, Command Carrier.
CAPITAL_SHIPYARD_GROUP_IDS: frozenset[int] = frozenset({485, 547, 883, 1538, 4594, 5120})
# Line 177 "Structure Supercapital Shipyard": those plus Titan and Supercarrier.
SUPERCAPITAL_SHIPYARD_GROUP_IDS: frozenset[int] = CAPITAL_SHIPYARD_GROUP_IDS | {30, 659}


def required_shipyard(group_id: int | None) -> str | None:
    """The shipyard a hull group needs (CAPITAL_SHIPYARD for a line-176 hull,
    SUPERCAPITAL_SHIPYARD for one only line 177 builds), None when the basic
    manufacturing line builds it."""
    if group_id in CAPITAL_SHIPYARD_GROUP_IDS:
        return CAPITAL_SHIPYARD
    if group_id in SUPERCAPITAL_SHIPYARD_GROUP_IDS:
        return SUPERCAPITAL_SHIPYARD
    return None


def services_providing(required: str) -> tuple[str, ...]:
    """The services whose assembly lines include every line `required` runs
    (a Supercapital Shipyard also runs the capital line 176)."""
    lines = set(SERVICE_ASSEMBLY_LINES[required])
    return tuple(s for s, own in SERVICE_ASSEMBLY_LINES.items() if lines <= set(own))


@dataclass(frozen=True)
class Decision:
    """allowed, why (every refusal says why; an allowance only when it rests on
    an assumption), and uncertain: the answer rests on unknown or assumed data
    and is logged as a WARNING."""
    allowed: bool
    reason: str | None = None
    uncertain: bool = False


def npc_space(profile: Any) -> bool | None:
    """True in NPC space (the system's constellation has a faction in the SDE),
    False when the profile says it has none, None when it does not say."""
    if not isinstance(profile, dict) or "system_faction_id" not in profile:
        return None
    return bool(profile.get("system_faction_id"))


def service_allowed(fit: Any, band: str | None, *, npc: bool | None = None, service: str = "") -> Decision:
    """Whether a structure's service (one entry of structure_service_fits) can
    run in the system: it fits the structure and may be onlined in the band.

    A service needing a sovereignty hub upgrade needs sovereign nullsec: not
    NPC nullsec (npc True), and refused when that is unknown (npc None). Whether
    the system's hub has the upgrade is not known here: it is assumed, and
    the allowance says so."""
    label = SERVICE_LABELS.get(service, "the service module")
    if not isinstance(fit, dict):
        return Decision(False, f"facility unknown (no SDE service-module data): {label} cannot be checked", True)
    if not fit.get("fits"):
        return Decision(False, f"{label} cannot be fitted to this structure (SDE)")
    band_rules = fit.get("disallow_in_high_sec") or fit.get("disallow_in_empire_space") or fit.get("requires_sov_upgrade")
    if not band_rules:
        return Decision(True)
    if band is None:
        return Decision(False, f"the structure's security band is unknown and {label} cannot be onlined in every band",
                        True)
    if fit.get("disallow_in_high_sec") and band == _security.HIGH:
        return Decision(False, f"{label} cannot be onlined in highsec (SDE disallowInHighSec)")
    if fit.get("disallow_in_empire_space") and band in _security.EMPIRE_BANDS:
        return Decision(False, f"{label} cannot be onlined in empire space (SDE disallowInEmpireSpace)")
    if fit.get("requires_sov_upgrade"):
        if band != _security.NULL:
            return Decision(False, f"{label} needs a sovereignty hub upgrade, which {band} space has not")
        if npc is None:
            return Decision(False, f"{label} needs a sovereignty hub upgrade and whether the system is NPC nullsec "
                                   "is unknown", True)
        if npc:
            return Decision(False, f"{label} needs a sovereignty hub upgrade: NPC nullsec has no sovereignty")
        return Decision(True, f"{label}: sovereignty hub upgrade assumed (not checkable from the SDE)", True)
    return Decision(True)
