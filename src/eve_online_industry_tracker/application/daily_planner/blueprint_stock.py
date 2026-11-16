"""Blueprint stock: which owned BPOs and inventable T2 blueprints have fewer
BPCs than the planner's minimum, counting the BPCs on their way, and in what
order the leftover science slots refill them (design section 7)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from eve_online_industry_tracker.application.daily_planner.models import ItemDecision
from eve_online_industry_tracker.application.daily_planner.values import attr as _job_attr
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.domain.industry_activities import ACTIVITY_COPYING, ACTIVITY_INVENTION

_DONE_STATUSES = ("delivered", "cancelled", "reverted")


@dataclass(frozen=True)
class StockGap:
    kind: str  # "copy" | "invent"
    blueprint_type_id: int  # the BPO to copy, or the T2 blueprint to invent
    source_blueprint_type_id: int | None  # T1 blueprint for "invent"
    have: float
    want: int
    product_type_id: int | None


def overview_probability(row: dict[str, Any]) -> float | None:
    """The product overview's invention chance (the producer's effective
    probability, else its plan's success probability); None when unknown."""
    chance = orow.get_invention_chance(row) or {}
    costs = orow.get_invention_costs(row) or {}
    for value in (chance.get("effective_probability"), costs.get("success_probability")):
        if isinstance(value, (int, float)) and not isinstance(value, bool) and 0 < value <= 1:
            return float(value)
    return None


def count_in_progress(
    industry_jobs: Iterable[Any], invention_probability: Callable[[int], float | None],
) -> dict[int, float]:
    """{blueprint type_id: BPCs on their way}: a copy job adds its runs
    (copies) to its blueprint, an invention job its runs x the probability
    to the blueprint it invents (its runs when the probability is unknown)."""
    out: dict[int, float] = {}
    for job in industry_jobs or ():
        if str(_job_attr(job, "status") or "").lower() in _DONE_STATUSES:
            continue
        activity = int(_job_attr(job, "activity_id") or 0)
        runs = int(_job_attr(job, "runs") or 0)
        if activity == ACTIVITY_COPYING:
            target, gain = int(_job_attr(job, "blueprint_type_id") or 0), float(runs)
        elif activity == ACTIVITY_INVENTION:
            target = int(_job_attr(job, "product_type_id") or 0)
            p = invention_probability(target) if target else None
            gain = runs * p if p is not None else float(runs)
        else:
            continue
        if target > 0 and gain > 0:
            out[target] = out.get(target, 0.0) + gain
    return out


def stock_gaps(
    *, bpo_type_ids: Iterable[int], bpc_counts: dict[int, int], in_progress: dict[int, float],
    invention_source_index: dict[int, int], min_bpc: int, min_invented: int,
    product_of: Callable[[int], int | None],
) -> list[StockGap]:
    """A "copy" gap per owned BPO type below `min_bpc` BPCs, an "invent" gap
    per T2 blueprint inventable from an owned BPO below `min_invented`."""
    owned = {int(t) for t in bpo_type_ids}

    def _have(bp: int) -> float:
        return bpc_counts.get(bp, 0) + in_progress.get(bp, 0.0)

    gaps = [
        StockGap("copy", bp, None, _have(bp), min_bpc, product_of(bp))
        for bp in sorted(owned) if _have(bp) < min_bpc
    ]
    gaps.extend(
        StockGap("invent", int(t2), int(src), _have(int(t2)), min_invented, product_of(int(t2)))
        for t2, src in sorted(invention_source_index.items())
        if int(src) in owned and _have(int(t2)) < min_invented
    )
    return gaps


def rank_gaps(gaps: list[StockGap], decision_by_product: dict[int, ItemDecision]) -> list[StockGap]:
    """Gaps of "build" rows first by capital return per day, then the other
    rows by margin, then those without a decision or margin; stable."""
    def _key(gap: StockGap) -> tuple[int, float]:
        decision = decision_by_product.get(gap.product_type_id) if gap.product_type_id is not None else None
        if decision is not None and decision.decision == "build":
            crpd = decision.capital_return_per_day
            return 0, -(crpd if crpd is not None else float("-inf"))
        margin = decision.margin_pct if decision is not None else None
        if isinstance(margin, (int, float)) and not isinstance(margin, bool):
            return 1, -float(margin)
        return 2, 0.0

    return sorted(gaps, key=_key)
