"""EVE's industry job install cost: one model for every activity.

    job_value = EIV basis x activity multiplier (the "process value")
    cost      = job_value x system cost index x (1 - structure cost reduction)
              + job_value x (facility tax + SCC rate of the activity)

The surcharges are charged on the job value, not on the index cost. The
EIV basis per activity (all from SDE base material quantities x the ESI
adjusted price, no ME):

- manufacturing / reaction: EIV of the job's materials x runs
- copying: copy multiplier (0.02) x EIV of the blueprint's manufacturing
  materials x total copied runs (copies x runs per copy)
- invention: invention multiplier (0.02) x EIV of the INVENTED blueprint's
  manufacturing materials x attempts
- ME/TE research: research multiplier (0.02) x EIV of the blueprint's
  manufacturing materials x the level multiplier (rank-1 cumulative level
  seconds / 105: 1 at level 1 ... 2438.1 at level 10), independent of the
  blueprint's rank

SCC is 4% of the job value for manufacturing, copying and invention, and
2% for ME/TE research. Fitted on real ESI jobs (Task 8b): manufacturing,
copying, invention and both research activities each reproduce ESI's cost
to a median 0.1% with the install-time index. Reactions follow the
manufacturing form (EVE's documented formula, not validated: no reaction
jobs to fit). A facility tax applies to the same job value (not validated:
the fitted structure charged none).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

RESEARCH_ACTIVITIES = frozenset({"research_material", "research_time"})

DEFAULT_SCC_SURCHARGE = 0.04
DEFAULT_RESEARCH_SCC_SURCHARGE = 0.02
DEFAULT_COPY_COST_EIV_MULTIPLIER = 0.02
DEFAULT_INVENTION_COST_EIV_MULTIPLIER = 0.02
DEFAULT_RESEARCH_COST_EIV_MULTIPLIER = 0.02

#: EVE's rank-1 cumulative research seconds from level 0 to each level.
RESEARCH_RANK1_LEVEL_SECONDS: dict[int, int] = {
    0: 0, 1: 105, 2: 250, 3: 595, 4: 1414, 5: 3360,
    6: 8000, 7: 19000, 8: 45255, 9: 107700, 10: 256000,
}


@dataclass(frozen=True)
class JobSurchargeRates:
    """The surcharge fractions charged on a job's value: the facility's tax
    plus CONCORD's SCC surcharge, which is lower for ME/TE research."""

    facility_tax: float = 0.0
    scc: float = DEFAULT_SCC_SURCHARGE
    research_scc: float = DEFAULT_RESEARCH_SCC_SURCHARGE

    def rate(self, activity: str) -> float:
        scc = self.research_scc if activity in RESEARCH_ACTIVITIES else self.scc
        return max(0.0, float(self.facility_tax)) + max(0.0, float(scc))


def research_level_multiplier(from_level: int, to_level: int) -> float:
    """EVE's dimensionless research cost multiplier from `from_level` to
    `to_level` (0..10): the rank-1 level seconds between them / 105. 0.0
    for an empty or out-of-range span."""
    table = RESEARCH_RANK1_LEVEL_SECONDS
    if int(from_level) not in table or int(to_level) not in table or int(to_level) <= int(from_level):
        return 0.0
    return float(table[int(to_level)] - table[int(from_level)]) / float(table[1])


def job_install_cost(
    *,
    activity: str,
    process_value: float | None,
    system_index: float,
    structure_cost_reduction: float,
    surcharge_rates: JobSurchargeRates,
) -> dict[str, Any]:
    """The install cost of one job whose job value is `process_value`.

    Keys: process_value, base_job_cost (value x index),
    job_cost_before_surcharge (after the structure reduction),
    installation_surcharge (value x the activity's surcharge rate) and
    total_job_cost. All None but process_value when the value is unknown or
    not positive, or the index is unknown (<= 0: the profile has no index for
    this activity; EVE's floor is positive, so 0 is never a real index)."""
    if process_value is None or float(process_value) <= 0 or float(system_index) <= 0:
        return {
            "process_value": process_value,
            "base_job_cost": None,
            "job_cost_before_surcharge": None,
            "installation_surcharge": None,
            "total_job_cost": None,
        }
    value = float(process_value)
    base_job_cost = value * float(system_index)
    discounted_job_cost = base_job_cost * max(0.0, 1.0 - float(structure_cost_reduction))
    surcharge_cost = value * surcharge_rates.rate(activity)
    return {
        "process_value": process_value,
        "base_job_cost": base_job_cost,
        "job_cost_before_surcharge": discounted_job_cost,
        "installation_surcharge": surcharge_cost,
        "total_job_cost": discounted_job_cost + surcharge_cost,
    }
