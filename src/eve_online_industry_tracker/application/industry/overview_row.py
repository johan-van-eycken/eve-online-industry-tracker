# src/eve_online_industry_tracker/application/industry/overview_row.py
"""Accessors for IndustryService product-overview rows.

An overview row is a plain dict produced by IndustryService. Its key names are
not guessable and several values live nested under `manufacturing_job`, so every
consumer — the Streamlit Industry Builder and the Daily Planner — reads rows
through this module. When the producer renames a key, one module breaks instead
of a scoring pipeline silently returning zeros.

Return conventions:
  * `get_*` returning a plain number never fails; absent means a documented
    neutral default (0 units, 0.0 % trend).
  * `get_*` returning `X | None` means the value is genuinely optional and
    every caller must decide what `None` implies.
"""
from __future__ import annotations

from typing import Any


def get_manufacturing_job(row: dict[str, Any]) -> dict[str, Any]:
    """The nested manufacturing-job dict, or `{}` when absent or malformed."""
    job = row.get("manufacturing_job")
    return job if isinstance(job, dict) else {}


def _as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_product_quantity(row: dict[str, Any]) -> int:
    """Units produced per batch. The producer writes this as `quantity`."""
    return _as_int(row.get("quantity"))


def get_effective_runs(row: dict[str, Any]) -> int:
    """Blueprint runs per batch, falling back to the product quantity."""
    runs = _as_int(get_manufacturing_job(row).get("runs"))
    if runs > 0:
        return runs
    return get_product_quantity(row)


def skill_requirements_met(row: dict[str, Any]) -> bool:
    """True when the producer's precomputed skill-requirements flag is set.

    The producer (`IndustryService._overview_row_skill_requirements_met`) sets a
    single precomputed boolean under `manufacturing_job.skills.skill_requirements_met`.
    This accessor only reads that flag — it does not itself iterate skill entries
    comparing `level` against `trained_skill_level`.
    """
    skills = get_manufacturing_job(row).get("skills") or {}
    if not isinstance(skills, dict):
        return False
    return bool(skills.get("skill_requirements_met", False))


def get_material_cost_total(row: dict[str, Any]) -> float | None:
    """Total material cost for one batch, or None when the producer has no price."""
    return _as_optional_float(get_manufacturing_job(row).get("material_cost"))


def get_total_cost(row: dict[str, Any]) -> float | None:
    """The batch's full build cost (materials + job costs), or None.

    The producer writes `manufacturing_job.total_cost = total_job_cost +
    material_cost` (industry/service.py, _enrich_product_rows_with_material_prices),
    and None when neither could be priced. total_job_cost covers the
    manufacturing install fee plus any planned copy, invention, source-copy,
    prerequisite and (SDE fallback) research job fees. A realized build cost
    carries only the manufacturing fee (+ invention cost for T2), so this is
    a close, not an exact, counterpart (see FeedbackProcessor).
    """
    return _as_optional_float(get_manufacturing_job(row).get("total_cost"))


def get_elapsed_time_seconds(row: dict[str, Any]) -> float | None:
    """Wall-clock time to finish one batch, for latency questions (will it
    sell before it is built, how long is capital tied up).

    Differs from `manufacturing_job.time_seconds` only for an invented T2 row:
    time_seconds holds the amortized expected slot time (a fraction of an
    invention attempt per run, which is right for ISK/h), elapsed_time_seconds
    the whole invention jobs actually needed (ceil(BPCs needed / probability)
    attempts in sequence) plus the source copy and the manufacturing.
    Falls back to time_seconds for a row without the field (older producer).
    None when the producer could not size the invention (unknown probability).
    """
    job = get_manufacturing_job(row)
    if "elapsed_time_seconds" in job:
        return _as_optional_float(job.get("elapsed_time_seconds"))
    return _as_optional_float(job.get("time_seconds"))


def get_batch_materials(row: dict[str, Any]) -> dict[int, int] | None:
    """{material type_id: units for the whole batch}, or None when unknown.

    `manufacturing_job.materials` is keyed by str(type_id), and each entry's
    `quantity` is what the producer computed for this batch: per-run quantity
    x runs, after the ME and structure material reduction
    (industry/service.py, the "Material adjustment" loop). Use it as-is; the
    SDE's per-run quantity is neither scaled nor reduced.

    None (not {}) when the producer wrote no materials mapping, so a caller
    cannot mistake "no data" for "this batch needs nothing". Malformed or
    non-positive entries are skipped.
    """
    materials = get_manufacturing_job(row).get("materials")
    if not isinstance(materials, dict):
        return None
    out: dict[int, int] = {}
    for entry in materials.values():
        if not isinstance(entry, dict):
            continue
        type_id = _as_int(entry.get("type_id"))
        quantity = _as_int(entry.get("quantity"))
        if type_id > 0 and quantity > 0:
            out[type_id] = out.get(type_id, 0) + quantity
    return out


def get_material_cost_per_unit(row: dict[str, Any]) -> float | None:
    """Material cost per produced unit.

    `manufacturing_job.material_cost` is a batch total; the producer itself
    divides it by the product quantity (industry/service.py:1770). None when
    either input is missing, so callers cannot silently treat it as free.
    """
    total = get_material_cost_total(row)
    if total is None:
        return None
    quantity = get_product_quantity(row)
    if quantity <= 0:
        return None
    return total / quantity


def get_pipeline_units_in_jobs(row: dict[str, Any]) -> int:
    return _as_int(row.get("pipeline_units_in_jobs"))


def get_pipeline_units_on_market(row: dict[str, Any]) -> int:
    return _as_int(row.get("pipeline_units_on_market"))


def get_pipeline_days_supply(row: dict[str, Any]) -> float | None:
    """Producer-computed days of pipeline supply; None when 7d volume was zero."""
    return _as_optional_float(row.get("pipeline_days_supply"))


def get_price_trend_7d_pct(row: dict[str, Any]) -> float:
    """7-day price trend percentage.

    Absent is deliberately read as `0.0` (flat), not `None`: this mirrors
    `PipelineState.price_trend_7d_pct`, which is declared as a required
    `float`, unlike `price_trend_30d_pct` (`float | None`, `None` meaning "not
    enough history"). Do not change this to return `None` on absence.
    """
    return _as_float(row.get("price_trend_7d_pct"))


def get_price_trend_30d_pct(row: dict[str, Any]) -> float | None:
    return _as_optional_float(row.get("price_trend_30d_pct"))


def get_days_of_supply(row: dict[str, Any]) -> float | None:
    return _as_optional_float(row.get("days_of_supply"))


def get_isk_per_hour(row: dict[str, Any]) -> float:
    return _as_float(row.get("isk_per_hour"))


def get_profit_amount(row: dict[str, Any]) -> float:
    return _as_float(row.get("profit_amount"))


def get_profit_margin_fraction(row: dict[str, Any]) -> float:
    return _as_float(row.get("profit_margin_fraction"))


def get_blueprint_type_id(row: dict[str, Any]) -> int | None:
    """Blueprint type id used to build this product.

    The producer writes this nested, under
    `manufacturing_job.blueprint_sde.blueprint_type_id`
    (industry/service.py:6921 sets it on `blueprint_sde_payload`; :7251 nests
    that payload under `manufacturing_job["blueprint_sde"]`). There is no
    top-level `blueprint_type_id` key on a real overview row, but one is
    checked as a fallback in case some other producer path writes it there
    directly. None when absent from both places, or not a positive int.
    """
    manufacturing_job = get_manufacturing_job(row)
    blueprint_sde = manufacturing_job.get("blueprint_sde")
    if isinstance(blueprint_sde, dict) and "blueprint_type_id" in blueprint_sde:
        raw = blueprint_sde.get("blueprint_type_id")
    else:
        raw = row.get("blueprint_type_id")
    value = _as_int(raw)
    return value if value > 0 else None


def get_meta_group_name(row: dict[str, Any]) -> str:
    """Meta group as a normalised display name.

    The producer writes a raw `meta_group_name` string (there is no meta group
    *id* on the row). This normalises variant spellings — including structure
    variants and the `abyssal` special-case, which maps to "Tech I" — into the
    canonical display names used by the UI. Unrecognised names pass through
    verbatim (after stripping).

    This mirrors `IndustryService._normalized_overview_meta_group_name`
    (industry/service.py:292) but is a separate, independently-maintained
    accessor — the two are not reconciled here.
    """
    raw_name = str(row.get("meta_group_name") or "").strip()
    normalized = raw_name.lower()
    if normalized in {"tech i", "structure tech i", "abyssal"}:
        return "Tech I"
    if normalized in {"tech ii", "structure tech ii"}:
        return "Tech II"
    if normalized in {"tech iii", "structure tech iii"}:
        return "Tech III"
    if normalized in {"faction", "structure faction"}:
        return "Faction"
    if normalized in {"storyline", "limited time"}:
        return "Storyline"
    return raw_name
