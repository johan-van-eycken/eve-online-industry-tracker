"""EVE's reprocessing yield.

    yield = (50 + Rm)/100 x (1 + Sec) x (1 + Sm)
            x (1 + 0.03 R) x (1 + 0.02 Re) x (1 + 0.02 Op) x (1 + Im)

- (50 + Rm)/100, the base yield of the ore's class (asteroid ore, moon ore,
  ice): an NPC station's own reprocessingEfficiency (npcStations; 0.50 at
  most stations, 0.25-0.40 at some) for every class; at a structure 0.50
  without a rig for the class (the Standup Reprocessing Facility service's
  refiningYieldNormalOres/MoonOres/Ice), 0.51 with a T1 rig and 0.53 with a
  T2 rig that covers the class (the rig's refiningYieldMultiplier). An
  Athanor needs an M-Set rig per class (Asteroid Ore, Moon Ore, Ice Grading
  Processor); a Tatara's L-Set Reprocessing Monitor covers all three.
- 1 + Sec, the rig's security modifier (hiSec/lowSec/nullSecModifier: 1.00,
  1.06, 1.12); only for a class with a rig.
- Sm, the structure's own bonus (strRefiningYieldBonus: Athanor 2%, Tatara
  5.5%); 0 at an NPC station.
- R, Re, Op: the Reprocessing, Reprocessing Efficiency and ore processing
  skill levels, each times its per-level bonus (the skill's
  refiningYieldMutator: 3%, 2%, 2%).
- Im, the implant (RX-801/802/804: 1/2/4%).

infrastructure.static_data.facility_repo builds the facility from the admin
settings and the SDE's dogma attributes; this module holds the arithmetic.
EVE rounds each material down per reprocessed batch (batch_output).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

FACILITY_NPC_STATION = "npc_station"
FACILITY_ATHANOR = "athanor"
FACILITY_TATARA = "tatara"
FACILITY_TYPES = (FACILITY_NPC_STATION, FACILITY_ATHANOR, FACILITY_TATARA)

RIG_NONE = "none"
RIG_T1 = "t1"
RIG_T2 = "t2"
RIGS = (RIG_NONE, RIG_T1, RIG_T2)

# The ore classes a reprocessing rig covers (infrastructure.sde.ore.ore_class).
ORE_CLASS_ASTEROID = "asteroid"
ORE_CLASS_MOON = "moon"
ORE_CLASS_ICE = "ice"
ORE_CLASSES = (ORE_CLASS_ASTEROID, ORE_CLASS_MOON, ORE_CLASS_ICE)

SECURITY_HIGH = "high"
SECURITY_LOW = "low"
SECURITY_NULL = "null"  # nullsec and wormhole space
SECURITY_BANDS = (SECURITY_HIGH, SECURITY_LOW, SECURITY_NULL)

# Where a facility's tax comes from.
TAX_SDE_STATION_BASE = "sde_station_base"            # npcStations.reprocessingStationsTake, before standings
TAX_SETTING_AFTER_STANDINGS = "setting_after_standings"  # an NPC station's rate the user set
TAX_SETTING = "setting"                              # a player structure's rate the user set


@dataclass(frozen=True)
class ClassYield:
    """One ore class's part of the facility yield."""
    base_yield: float            # (50 + Rm)/100
    security_multiplier: float   # 1 + Sec (1.0 without a rig for the class)
    rig: str = RIG_NONE          # the rig covering the class: RIG_NONE, RIG_T1 or RIG_T2


@dataclass(frozen=True)
class ReprocessingFacility:
    name: str
    yields: Mapping[str, ClassYield]   # ORE_CLASSES -> the class's base yield and security modifier
    structure_bonus: float       # Sm, a fraction
    tax: float                   # the facility's reprocessing tax, a fraction
    tax_source: str              # TAX_SDE_STATION_BASE, TAX_SETTING_AFTER_STANDINGS or TAX_SETTING

    def multiplier_for(self, ore_class: str) -> float:
        """(50 + Rm)/100 x (1 + Sec) x (1 + Sm) for an ore of this class."""
        try:
            part = self.yields[ore_class]
        except KeyError:
            raise ValueError(f"no reprocessing yield for ore class {ore_class!r} at {self.name}") from None
        return part.base_yield * part.security_multiplier * (1.0 + self.structure_bonus)


def reprocessing_yield(facility: ReprocessingFacility, *, ore_class: str, skill_bonuses: Mapping[int, float],
                       skill_levels: Mapping[int, int], implant_bonus: float) -> float:
    """The yield fraction for an ore of `ore_class`. skill_bonuses is {skill
    type id: bonus per level, a fraction} for each skill that applies
    (Reprocessing, Reprocessing Efficiency, the ore's skill); skill_levels the
    character's trained levels by type id (an untrained skill counts 0)."""
    result = facility.multiplier_for(ore_class)
    for skill_id, per_level in skill_bonuses.items():
        result *= 1.0 + per_level * int(skill_levels.get(skill_id, 0))
    return result * (1.0 + implant_bonus)


def batch_output(quantity: int, yield_fraction: float) -> int:
    """Units of a material one batch gives: rounded down, as EVE does
    (400 x 0.7236 = 289). The tiny epsilon keeps an exact product (100 x 0.5)
    from losing a unit to floating point."""
    return int(math.floor(quantity * yield_fraction + 1e-9))
