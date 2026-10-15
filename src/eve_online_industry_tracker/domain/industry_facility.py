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
"""
from __future__ import annotations

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


def service_allowed(fit: Any, band: str | None) -> tuple[bool, str | None]:
    """(allowed, reason) for one service of a structure in a band.

    reason is set when the answer is not known for sure and the service is
    therefore refused: no fitting data, or the band is unknown while the
    module's onlining depends on it."""
    if not isinstance(fit, dict):
        return False, "the SDE fitting data for the structure's service modules is unknown"
    if not fit.get("fits"):
        return False, None
    band_rules = fit.get("disallow_in_high_sec") or fit.get("disallow_in_empire_space") or fit.get("requires_sov_upgrade")
    if not band_rules:
        return True, None
    if band is None:
        return False, "the structure's security band is unknown and the service module cannot be onlined in every band"
    if fit.get("disallow_in_high_sec") and band == _security.HIGH:
        return False, None
    if fit.get("disallow_in_empire_space") and band in _security.EMPIRE_BANDS:
        return False, None
    if fit.get("requires_sov_upgrade") and band != _security.NULL:
        # A sovereignty hub upgrade exists only in sovereign nullsec (not in
        # wormhole space). Whether this system's hub has it is not known
        # here; the band is the part the SDE can check.
        return False, None
    return True, None
