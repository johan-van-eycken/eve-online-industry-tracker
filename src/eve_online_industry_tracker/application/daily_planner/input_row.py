# src/eve_online_industry_tracker/application/daily_planner/input_row.py
"""The Daily Planner's input contract over IndustryService overview rows.

This is the only place in the daily_planner package that reads a raw overview
dict. Everything downstream consumes PlannerInputRow, so a producer key rename
raises here — naming the field and the type_id — instead of silently zeroing a
score eight phases later.

Required vs optional, and a third category this module treats deliberately:
  * required — the *key* must be present; the planner cannot make a
    meaningful decision without it, and if it is missing that is almost
    certainly a producer rename, not legitimate business data.
  * optional (`X | None`) — the *key* may be entirely absent and that is
    normal; every consumer must handle `None` explicitly.
  * required-key, optional-value (`isk_per_hour`, `profit_amount`,
    `profit_margin_fraction`) — IndustryService._enrich_product_rows_with_profit_metrics
    (industry/service.py:2245-2296) unconditionally assigns these three keys
    on every row, but their *value* is legitimately `None` whenever an input
    to the calculation is itself unavailable: `net_proceeds` or
    `manufacturing_job.total_cost` missing leaves `profit_amount` `None`;
    `net_proceeds <= 0` leaves `profit_margin_fraction` `None`; a missing or
    zero `time_seconds` leaves `isk_per_hour` `None`. None of that is a
    producer bug, so raising on it would abort plan computation on ordinary
    data. What *would* be a bug is the key not existing on the row at all
    (e.g. a rename to `isk_per_hr`) — that still raises, naming the field,
    exactly like any other required field. Every consumer of these three
    fields must handle `None` explicitly, the same as the purely-optional
    fields below.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from eve_online_industry_tracker.application.industry import overview_row as orow


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
    material_cost_per_unit: float
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

        type_name = str(row.get("type_name") or "").strip()
        if not type_name:
            raise PlannerInputError(type_id=type_id, field="type_name", detail="is missing")

        quantity = _require_int(type_id, "quantity", row.get("quantity"), positive=True)

        if orow.get_material_cost_total(row) is None:
            raise PlannerInputError(
                type_id=type_id,
                field="manufacturing_job.material_cost",
                detail="is missing — material cost cannot default to zero",
            )
        material_cost_per_unit = orow.get_material_cost_per_unit(row)
        if material_cost_per_unit is None:
            raise PlannerInputError(
                type_id=type_id,
                field="manufacturing_job.material_cost",
                detail="could not be converted to a per-unit cost",
            )

        return cls(
            type_id=type_id,
            type_name=type_name,
            quantity=quantity,
            runs=orow.get_effective_runs(row),
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
