# src/eve_online_industry_tracker/application/daily_planner/input_row.py
"""The Daily Planner's input contract over IndustryService overview rows.

This is the only place in the daily_planner package that reads a raw overview
dict. Everything downstream consumes PlannerInputRow.

Only six fields actually guard against a producer key rename by raising when
the key is entirely absent: `type_id`, `quantity`, `manufacturing_job.runs`,
`isk_per_hour`, `profit_amount` and `profit_margin_fraction`. Every other
field is read through `overview_row` accessors (or, for `material_cost`, a
local reader with the same convention) that quietly default or return `None`
on an absent key -- that is by design for those fields (see below), not an
oversight, but it does mean a rename of e.g. `pipeline_days_supply` or
`blueprint_type_id` would NOT be caught here.

Four categories, not two:
  * required, key must be present (`type_id`, `quantity`,
    `manufacturing_job.runs`) -- the planner cannot make a meaningful
    decision without it, and a missing key here is almost certainly a
    producer rename, not legitimate business data. `runs` deliberately does
    NOT fall back to `quantity` the way `overview_row.get_effective_runs`
    does for the UI: `quantity` on the top-level row is
    `product_quantity_per_run * effective_runs` (industry/service.py:6916),
    a batch-units total, not a run count, so silently substituting it for a
    renamed `runs` key would multiply material quantities by the wrong
    factor downstream (the exact "buys materials for one run on a 20-run
    job" defect this contract exists to prevent).
  * required key, optional value (`isk_per_hour`, `profit_amount`,
    `profit_margin_fraction`) -- IndustryService._enrich_product_rows_with_profit_metrics
    (industry/service.py:2245-2296) unconditionally assigns these three keys
    on every row, but their *value* is legitimately `None` whenever an input
    to the calculation is itself unavailable: `net_proceeds` or
    `manufacturing_job.total_cost` missing leaves `profit_amount` `None`;
    `net_proceeds <= 0` leaves `profit_margin_fraction` `None`; a missing or
    zero `time_seconds` leaves `isk_per_hour` `None`. None of that is a
    producer bug, so raising on it would abort plan computation on ordinary
    data. What *would* be a bug is the key not existing on the row at all
    (e.g. a rename to `isk_per_hr`) -- that still raises, naming the field.
    Every consumer of these three fields must handle `None` explicitly.
  * optional, key and value (`pipeline_days_supply`, `price_trend_30d_pct`,
    `days_of_supply`, `meta_group_id`, `blueprint_type_id`) -- genuinely
    absent sometimes; every consumer must handle `None`. `manufacturing_job`
    `.material_cost` also belongs here even though its per-row derived field
    is named `material_cost_per_unit`: `_enrich_product_rows_with_material_prices`
    (industry/service.py:7413, :7432, :7454, :7457) returns or `continue`s
    before ever setting `material_cost` whenever pricing could not be
    computed, so an absent or `None` `material_cost` is ordinary data and
    yields `material_cost_per_unit = None`, not a raise. A malformed
    `manufacturing_job` (present but not a dict) or a present-but-non-numeric
    `material_cost` still raises -- those are not things the producer does.
  * display-only, never raises (`type_name`) -- `_enrich_type_entry`
    (infrastructure/sde/blueprints.py) falls back to `{"type_id": type_id}`
    when a type is missing from the SDE, so `type_name` may simply not be
    there; the producer itself defends against this with `type_name or
    type_id` (industry/service.py:2526, :7193). This module does the same:
    falls back to `str(type_id)` instead of raising, because losing the
    rename guard on a display-only field is a fair trade for not taking the
    whole plan offline over one un-named product.

`raw` is a live, shallow reference to the caller's row dict, not a copy.
`chain_planner.py` (~lines 176-246) deliberately writes decision keys (e.g.
`needs_me_research`, `needs_invention`) back into `decision.overview_row`,
which is this same dict -- that mutation is load-bearing for the
chain-planner-to-character-assigner handoff. Do NOT deep-copy `raw`;
`frozen=True` on this dataclass only prevents reassigning its own fields, it
does not and must not prevent later phases from mutating the dict `raw`
points at.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from eve_online_industry_tracker.application.industry import overview_row as orow

logger = logging.getLogger(__name__)


class PlannerInputError(ValueError):
    """An overview row does not satisfy the planner's input contract."""

    def __init__(self, *, type_id: Any, field: str, detail: str) -> None:
        self.type_id = type_id
        self.field = field
        self.detail = detail
        super().__init__(f"overview row type_id={type_id}: {field} {detail}")


class MetaGroupSource(Protocol):
    def meta_group_id(self, type_id: int) -> int | None: ...


def _require_int(type_id: Any, name: str, value: Any, *, positive: bool = False) -> int:
    if value is None:
        raise PlannerInputError(type_id=type_id, field=name, detail="is missing")
    try:
        out = int(value)
    except (TypeError, ValueError):
        raise PlannerInputError(
            type_id=type_id, field=name, detail=f"is not an integer: {value!r}"
        ) from None
    if positive and out <= 0:
        raise PlannerInputError(type_id=type_id, field=name, detail=f"must be > 0, got {out}")
    return out


def _require_key_optional_float(type_id: Any, name: str, row: dict[str, Any]) -> float | None:
    """The key must exist on the row, but its value may legitimately be None.

    Used for `isk_per_hour`, `profit_amount` and `profit_margin_fraction`:
    the producer always writes the key, but the value itself is None when an
    upstream input (net proceeds, total cost, time) is unavailable. A missing
    *key* (a rename) still raises, naming the field; a present `None` value
    passes through unchanged.
    """
    if name not in row:
        raise PlannerInputError(type_id=type_id, field=name, detail="is missing")
    value = row[name]
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        raise PlannerInputError(
            type_id=type_id, field=name, detail=f"is not a number: {value!r}"
        ) from None


@dataclass(frozen=True)
class PlannerInputRow:
    """One overview row, validated and normalised for the planner."""

    type_id: int
    type_name: str
    quantity: int
    runs: int
    material_cost_per_unit: float | None
    isk_per_hour: float | None
    profit_amount: float | None
    profit_margin_fraction: float | None
    pipeline_units_in_jobs: int
    pipeline_units_on_market: int
    price_trend_7d_pct: float
    pipeline_days_supply: float | None
    price_trend_30d_pct: float | None
    days_of_supply: float | None
    meta_group_id: int | None
    blueprint_type_id: int | None
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def from_overview(cls, row: Any, *, meta_groups: MetaGroupSource) -> "PlannerInputRow":
        if not isinstance(row, dict):
            raise PlannerInputError(
                type_id=None, field="<row>", detail=f"is not a dict: {type(row).__name__}"
            )

        # When the id itself is missing there is nothing better to report as
        # the error's type_id than the (missing) value being validated.
        raw_type_id = row.get("type_id")
        type_id = _require_int(raw_type_id, "type_id", raw_type_id, positive=True)

        # Display-only: the SDE can lack a name for a type (blueprints.py
        # _enrich_type_entry's {"type_id": type_id}-only fallback), and the
        # producer itself tolerates that (service.py:2526, :7193). Falling
        # back to the id string, rather than raising, keeps one unnamed
        # product from taking the whole plan offline.
        type_name = str(row.get("type_name") or "").strip() or str(type_id)

        quantity = _require_int(type_id, "quantity", row.get("quantity"), positive=True)

        # manufacturing_job must be a dict if present at all -- a malformed
        # container (present but wrong type) raises once here, naming the
        # container itself, rather than surfacing as a confusing failure to
        # read "runs" or "material_cost" out of it below.
        manufacturing_job_raw = row.get("manufacturing_job")
        if manufacturing_job_raw is not None and not isinstance(manufacturing_job_raw, dict):
            raise PlannerInputError(
                type_id=type_id,
                field="manufacturing_job",
                detail=f"is not a dict: {manufacturing_job_raw!r}",
            )
        manufacturing_job: dict[str, Any] = (
            manufacturing_job_raw if isinstance(manufacturing_job_raw, dict) else {}
        )

        # runs is required with NO fallback to quantity: quantity on the
        # top-level row is product_quantity_per_run * effective_runs (a
        # batch-units total), so substituting it for a renamed runs key
        # would silently multiply material quantities by the wrong factor
        # downstream. overview_row.get_effective_runs's quantity fallback is
        # correct for the UI's own use and is intentionally not reused here.
        if "runs" not in manufacturing_job:
            raise PlannerInputError(
                type_id=type_id, field="manufacturing_job.runs", detail="is missing"
            )
        runs = _require_int(
            type_id, "manufacturing_job.runs", manufacturing_job.get("runs"), positive=True
        )

        # material_cost: absent key or an explicit None both mean "pricing
        # could not be computed yet" (industry/service.py:7413, :7432, :7454,
        # :7457 return/continue before ever setting it) -- ordinary data,
        # not a rename, so it yields material_cost_per_unit = None rather
        # than raising. A present-but-non-numeric value still raises.
        if "material_cost" not in manufacturing_job or manufacturing_job.get("material_cost") is None:
            material_cost_total: float | None = None
        else:
            raw_cost = manufacturing_job["material_cost"]
            try:
                material_cost_total = float(raw_cost)
            except (TypeError, ValueError):
                raise PlannerInputError(
                    type_id=type_id,
                    field="manufacturing_job.material_cost",
                    detail=f"is not a number: {raw_cost!r}",
                ) from None
        material_cost_per_unit = (
            material_cost_total / quantity if material_cost_total is not None else None
        )

        return cls(
            type_id=type_id,
            type_name=type_name,
            quantity=quantity,
            runs=runs,
            material_cost_per_unit=material_cost_per_unit,
            isk_per_hour=_require_key_optional_float(type_id, "isk_per_hour", row),
            profit_amount=_require_key_optional_float(type_id, "profit_amount", row),
            profit_margin_fraction=_require_key_optional_float(
                type_id, "profit_margin_fraction", row
            ),
            pipeline_units_in_jobs=orow.get_pipeline_units_in_jobs(row),
            pipeline_units_on_market=orow.get_pipeline_units_on_market(row),
            price_trend_7d_pct=orow.get_price_trend_7d_pct(row),
            pipeline_days_supply=orow.get_pipeline_days_supply(row),
            price_trend_30d_pct=orow.get_price_trend_30d_pct(row),
            days_of_supply=orow.get_days_of_supply(row),
            meta_group_id=meta_groups.meta_group_id(type_id),
            blueprint_type_id=orow.get_blueprint_type_id(row),
            raw=row,
        )


def _variant_rank(row: PlannerInputRow) -> float | None:
    """The isk/hour a variant would be scored on, or None when it can't be.

    Mirrors ProfitabilityScorer's two unscoreable conditions: no isk/hour, or
    no cost basis at all (neither a material cost nor the producer's profit).
    """
    if row.isk_per_hour is None:
        return None
    if row.material_cost_per_unit is None and row.profit_amount is None:
        return None
    return row.isk_per_hour


def _row_label(row: PlannerInputRow, index: int) -> str:
    row_id = row.raw.get("overview_row_id") if isinstance(row.raw, dict) else None
    return str(row_id) if row_id is not None else f"index {index}"


def dedupe_by_type_id(rows: list[PlannerInputRow]) -> list[PlannerInputRow]:
    """Keep ONE row per type_id: the scoreable variant with the highest isk/hour.

    The producer emits one overview row per blueprint variant, so a product
    can appear several times. Every phase after this one keys by type_id, and
    with duplicates some lookups kept the first row and others the last: one
    product got several decisions, each mixing one variant's pipeline state
    with another's profitability. Unscoreable variants (see _variant_rank)
    rank last; ties keep the first row seen. Order follows each product's
    first appearance. Every drop is logged at WARNING with the overview_row_id
    (or list index) of the kept and the dropped rows, because the cost of a
    wrong pick is the user losing a variant they preferred.
    """
    best: dict[int, tuple[int, PlannerInputRow]] = {}
    dropped: dict[int, list[str]] = {}
    for index, row in enumerate(rows):
        current = best.get(row.type_id)
        if current is None:
            best[row.type_id] = (index, row)
            continue
        cur_index, cur_row = current
        new_rank, cur_rank = _variant_rank(row), _variant_rank(cur_row)
        if new_rank is not None and (cur_rank is None or new_rank > cur_rank):
            dropped.setdefault(row.type_id, []).append(_row_label(cur_row, cur_index))
            best[row.type_id] = (index, row)
        else:
            dropped.setdefault(row.type_id, []).append(_row_label(row, index))

    for type_id, labels in dropped.items():
        kept_index, kept = best[type_id]
        logger.warning(
            "Daily planner: type_id=%s (%s) has %d overview rows (one per blueprint "
            "variant); keeping %s (isk/hour %s), dropping %s",
            type_id, kept.type_name, len(labels) + 1, _row_label(kept, kept_index),
            kept.isk_per_hour, ", ".join(labels),
        )
    # dict order is first insertion, so products keep their first-seen order.
    return [row for _, row in best.values()]
