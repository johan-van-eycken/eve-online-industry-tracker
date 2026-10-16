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

Which hulls need a shipyard is CCP's assembly-line data: the SDE's
industryAssemblyLines and industryInstallationTypes (build 3607705 on),
read by infrastructure/sde/industry into ShipyardLines (below).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

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

# The services that run manufacturing lines, cheapest first.
_MANUFACTURING_SERVICES = (MANUFACTURING_PLANT, CAPITAL_SHIPYARD, SUPERCAPITAL_SHIPYARD)


@dataclass(frozen=True)
class ShipyardLines:
    """Which assembly lines each manufacturing service module runs, and the
    hull groups each line builds, from the SDE (build_shipyard_lines).

    At the time of writing: the Manufacturing Plant runs line 175 "Structure
    Basic Manufacturing" (every other manufacturable hull, Freighters and
    Jump Freighters included), the Capital Shipyard line 176 (Dreadnought,
    Carrier, Capital Industrial Ship, Force Auxiliary, Lancer Dreadnought,
    Command Carrier), the Supercapital Shipyard lines 176 and 177 (those
    plus Titan and Supercarrier)."""
    service_lines: Mapping[str, frozenset[int]]
    line_groups: Mapping[int, frozenset[int]]

    def _groups(self, service: str) -> frozenset[int]:
        return frozenset(g for line in self.service_lines[service] for g in self.line_groups[line])

    @property
    def capital_group_ids(self) -> frozenset[int]:
        """Hull groups the Capital Shipyard builds and the basic line does not."""
        return self._groups(CAPITAL_SHIPYARD) - self._groups(MANUFACTURING_PLANT)

    @property
    def supercapital_group_ids(self) -> frozenset[int]:
        """Hull groups the Supercapital Shipyard builds and the basic line does not."""
        return self._groups(SUPERCAPITAL_SHIPYARD) - self._groups(MANUFACTURING_PLANT)


def build_shipyard_lines(
    assembly_lines: Mapping[int, Mapping[str, Any]], installation_types: Mapping[int, Mapping[str, Any]], *,
    group_categories: Mapping[int, int],
) -> ShipyardLines:
    """ShipyardLines from the SDE's industryAssemblyLines and
    industryInstallationTypes ({id: row} as the SDE YAML has them) and the
    SDE's group -> category map. A line builds the groups its
    detailsPerGroup lists and every group in a category its
    detailsPerCategory lists. A manufacturing service module the
    installation types do not list, a line they name that the assembly lines
    lack, or a service line with a detailsPerTypeList (types, which the
    hull-group gate cannot read) is a ValueError: the gate cannot be told
    from such data."""
    service_lines: dict[str, frozenset[int]] = {}
    for service in _MANUFACTURING_SERVICES:
        lines: set[int] = set()
        for type_id in SERVICE_MODULE_TYPE_IDS[service]:
            row = installation_types.get(type_id)
            if row is None:
                raise ValueError(f"SDE industryInstallationTypes lacks service module {type_id} ({SERVICE_LABELS[service]})")
            lines |= {int(e["assemblyLineID"]) for e in row.get("assemblyLines") or ()}
        service_lines[service] = frozenset(lines)
    line_groups: dict[int, frozenset[int]] = {}
    for line_id in sorted({line for lines in service_lines.values() for line in lines}):
        row = assembly_lines.get(line_id)
        if row is None:
            raise ValueError(f"SDE industryAssemblyLines lacks line {line_id}, which a service module runs")
        if row.get("detailsPerTypeList"):
            raise ValueError(f"SDE industryAssemblyLines line {line_id}, which a service module runs, has a "
                             "detailsPerTypeList; the shipyard gate reads groups and categories only")
        categories = {int(d["categoryID"]) for d in row.get("detailsPerCategory") or ()}
        line_groups[line_id] = frozenset(
            {int(d["groupID"]) for d in row.get("detailsPerGroup") or ()}
            | {int(g) for g, c in group_categories.items() if c is not None and int(c) in categories}
        )
    return ShipyardLines(service_lines, line_groups)


def required_shipyard(group_id: int | None, lines: ShipyardLines) -> str | None:
    """The shipyard a hull group needs (CAPITAL_SHIPYARD for a hull the
    Capital Shipyard's lines build, SUPERCAPITAL_SHIPYARD for one only the
    Supercapital Shipyard's build), None when the basic manufacturing line
    builds it (or no shipyard line names it)."""
    if group_id in lines.capital_group_ids:
        return CAPITAL_SHIPYARD
    if group_id in lines.supercapital_group_ids:
        return SUPERCAPITAL_SHIPYARD
    return None


def services_providing(required: str, lines: ShipyardLines) -> tuple[str, ...]:
    """The services whose assembly lines include every line `required` runs
    (a Supercapital Shipyard also runs the capital line)."""
    needed = lines.service_lines[required]
    return tuple(s for s in _MANUFACTURING_SERVICES if needed <= lines.service_lines[s])


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
