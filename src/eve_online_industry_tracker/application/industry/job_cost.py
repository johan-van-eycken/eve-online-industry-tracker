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
2% for ME/TE research. Fitted on real ESI jobs: manufacturing,
copying, invention and both research activities each reproduce ESI's cost
to a median 0.1% with the install-time index. Reactions follow the
manufacturing form (EVE's documented formula, not validated: no reaction
jobs to fit). A facility tax applies to the same job value (not validated:
the fitted structure charged none).
"""
from __future__ import annotations

import math
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
    plus CONCORD's SCC surcharge, which is lower for ME/TE research.
    `facility_tax` None = unknown (`unknown_reason` says why): every fee is
    then unknown, never priced at a guessed 0 % tax."""

    facility_tax: float | None = 0.0
    scc: float = DEFAULT_SCC_SURCHARGE
    research_scc: float = DEFAULT_RESEARCH_SCC_SURCHARGE
    unknown_reason: str | None = None

    def rate(self, activity: str) -> float | None:
        if self.facility_tax is None:
            return None
        scc = self.research_scc if activity in RESEARCH_ACTIVITIES else self.scc
        return max(0.0, float(self.facility_tax)) + max(0.0, float(scc))


def profile_facility_tax(profile: dict[str, Any] | None, *, scc: float) -> tuple[float | None, str | None]:
    """(facility tax, None), or (None, why) when the industry profile does
    not determine it. A profile's `facility_tax` is used as stored. A legacy
    profile without it falls back to installation_cost_modifier (facility
    tax + SCC) minus the SCC it was saved with (its `scc_surcharge`, else
    `scc`); a modifier below that SCC is inconsistent, so unknown."""
    if not isinstance(profile, dict):
        return None, "no industry profile selected"
    stored = profile.get("facility_tax")
    if stored is not None:
        try:
            tax = float(stored)
        except (TypeError, ValueError):
            return None, f"the industry profile's facility tax {stored!r} is not a number"
        if not math.isfinite(tax) or tax < 0:
            return None, f"the industry profile's facility tax {tax} is not a valid rate"
        return tax, None
    modifier = profile.get("installation_cost_modifier")
    if modifier is None:
        return None, "the industry profile has no facility tax (re-save the profile)"
    profile_scc = profile.get("scc_surcharge")
    try:
        modifier_f = float(modifier)
        base_scc = float(profile_scc) if profile_scc is not None else float(scc)
    except (TypeError, ValueError):
        return None, "the industry profile's installation cost modifier is not a number"
    tax = modifier_f - base_scc
    if not math.isfinite(tax) or tax < -1e-12:
        return None, (
            f"the industry profile's installation cost modifier ({modifier_f:.2%}) is below the "
            f"SCC surcharge ({base_scc:.2%}), so its facility tax is unknown (re-save the profile)"
        )
    return max(0.0, tax), None


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
    system_index: float | None,
    structure_cost_reduction: float,
    surcharge_rates: JobSurchargeRates,
    index_unknown_reason: str | None = None,
    eiv_unknown_reason: str | None = None,
) -> dict[str, Any]:
    """The install cost of one job whose job value is `process_value`.

    Keys: process_value, base_job_cost (value x index),
    job_cost_before_surcharge (after the structure reduction),
    installation_surcharge (value x the activity's surcharge rate),
    total_job_cost, and fee_unknown_reason (None when priced). The fee is
    unknown (all None but process_value, with the reason) when the value is
    unknown or not positive (a material without a price, or a non-numeric
    quantity: `eiv_unknown_reason`), the index is unknown (None, with
    `index_unknown_reason`, or <= 0: EVE's floor is positive, so 0 is never a
    real index), or the facility tax is unknown."""
    rate = surcharge_rates.rate(activity)
    reason: str | None = None
    if process_value is None or float(process_value) <= 0:
        reason = f"no priced EIV for the {activity} job ({eiv_unknown_reason or 'a material has no adjusted price'})"
    elif system_index is None or float(system_index) <= 0:
        reason = index_unknown_reason or f"the industry profile has no {activity} cost index"
    elif rate is None:
        reason = surcharge_rates.unknown_reason or "the facility tax is unknown"
    if reason is not None or rate is None:
        return {
            "process_value": process_value,
            "base_job_cost": None,
            "job_cost_before_surcharge": None,
            "installation_surcharge": None,
            "total_job_cost": None,
            "fee_unknown_reason": reason,
        }
    value = float(process_value)
    base_job_cost = value * float(system_index or 0.0)
    discounted_job_cost = base_job_cost * max(0.0, 1.0 - float(structure_cost_reduction))
    surcharge_cost = value * rate
    return {
        "process_value": process_value,
        "base_job_cost": base_job_cost,
        "job_cost_before_surcharge": discounted_job_cost,
        "installation_surcharge": surcharge_cost,
        "total_job_cost": discounted_job_cost + surcharge_cost,
        "fee_unknown_reason": None,
    }


def job_fee_rates_summary(profile: dict[str, Any] | None, *, scc: float, research_scc: float) -> str:
    """One line for the industry profile page: the facility tax and the two
    surcharge totals the producer charges on a job's value (manufacturing /
    reaction / copy / invention, and ME/TE research), from the admin SCC
    rates; or why the fees are unknown."""
    tax, reason = profile_facility_tax(profile, scc=scc)
    if tax is None:
        return f"Job fees unknown: {reason}"
    rates = JobSurchargeRates(facility_tax=tax, scc=scc, research_scc=research_scc)
    return (
        f"Facility tax {tax:.2%} | Manufacturing / reaction / copy / invention total "
        f"{rates.rate('manufacturing'):.2%} (SCC {scc:.2%}) | "
        f"ME/TE research total {rates.rate('research_material'):.2%} (SCC {research_scc:.2%})"
    )
