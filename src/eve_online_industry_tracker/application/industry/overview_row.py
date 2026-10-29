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


def get_facility_gate_reason(row: dict[str, Any]) -> str | None:
    """Why the overview's industry profile cannot build this row (e.g. "needs
    Standup Capital Shipyard I: ..."), or None. None also for a row cached
    before the gate was stamped: it is gated from the next refresh on."""
    reason = row.get("facility_gate_reason")
    return str(reason) if reason else None


def get_profit_upper_bound_reason(row: dict[str, Any]) -> str | None:
    """"profit is an upper bound: job fees unknown for: ..." when the producer
    left unpriced job fees out of the row's total_cost (then
    profit, margin and ISK/h are upper bounds and total_cost a lower bound),
    else None. The one wording of the planner and the decision reason."""
    reasons = get_manufacturing_job(row).get("job_fee_unknown_reasons")
    if not reasons:
        return None
    return f"profit is an upper bound: job fees unknown for: {'; '.join(str(r) for r in reasons)}"


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


def get_invention_required_skill_levels(row: dict[str, Any]) -> dict[int, int] | None:
    """{skill type_id: required level} of the row's invention skills
    (manufacturing_job.invention_chance.required_skill_levels), or
    None when the producer published none: an older row
    (callers then require level 1 of every skill in the set)."""
    chance = get_manufacturing_job(row).get("invention_chance")
    levels = chance.get("required_skill_levels") if isinstance(chance, dict) else None
    if not isinstance(levels, dict) or not levels:
        return None
    out: dict[int, int] = {}
    for raw_id, raw_level in levels.items():
        type_id, level = _as_int(raw_id), _as_int(raw_level)
        if type_id <= 0 or level <= 0:
            return None
        out[type_id] = level
    return out


def get_invention_skill_set(row: dict[str, Any]) -> tuple[tuple[int, int], int | None] | None:
    """((science skill ids), encryption skill id) the producer's invention
    chance read for this row (manufacturing_job.invention_chance), or None
    when the row carries none or the skills could not be identified.

    The encryption id is None for a blueprint whose SDE entry lists no
    encryption skill (`encryption_skill_type_id: None` with two
    science ids): ranked by the science skills only."""
    chance = get_manufacturing_job(row).get("invention_chance")
    if not isinstance(chance, dict):
        return None
    science = chance.get("science_skill_type_ids")
    if not isinstance(science, list) or len(science) != 2:
        return None
    first, second = (_as_int(s) for s in science)
    if first <= 0 or second <= 0:
        return None
    raw_encryption = chance.get("encryption_skill_type_id")
    if raw_encryption is None:
        return (first, second), None
    encryption = _as_int(raw_encryption)
    if encryption <= 0:
        return None
    return (first, second), encryption


def get_invention_chance(row: dict[str, Any]) -> dict[str, Any] | None:
    """The producer's invention chance breakdown for this row's own invention
    (manufacturing_job.invention_chance, see invention_model.invention_chance_payload),
    or None when the row carries none."""
    chance = get_manufacturing_job(row).get("invention_chance")
    return chance if isinstance(chance, dict) else None


def get_invention_costs(row: dict[str, Any]) -> dict[str, Any] | None:
    """The row's own (top-level) invention plan, best inventor:
    manufacturing_job.activity_breakdown.invention (expected attempts in
    `runs`, `whole_attempts`, `invented_runs`, `runs_per_invented_blueprint`,
    `success_probability`, the fee in `total_job_cost`). None when the row
    invents nothing itself."""
    breakdown = get_manufacturing_job(row).get("activity_breakdown")
    invention = breakdown.get("invention") if isinstance(breakdown, dict) else None
    return invention if isinstance(invention, dict) else None


def get_own_expected_invention_inputs(row: dict[str, Any]) -> list[dict[str, Any]]:
    """The row's own expected (amortized) invention inputs, datacores and
    decryptor, as the producer priced them (quantity, unit_price, line_total;
    see IndustryService._expected_invention_inputs): every
    manufacturing_job.expected_invention_materials entry that is not a nested
    sub-build's (those carry invented_blueprint_type_id). [] when none."""
    entries = get_manufacturing_job(row).get("expected_invention_materials")
    if not isinstance(entries, dict):
        return []
    return [e for e in entries.values() if isinstance(e, dict) and not e.get("invented_blueprint_type_id")]


def get_invention_bpc_plan(row: dict[str, Any]) -> dict[str, Any] | None:
    """The later-batch invention plan (manufacturing_job.invention_bpc_plan:
    batch_runs, runs_per_invented_blueprint, whole_attempts_per_blueprint,
    ...), written when an owned T2 BPC covers batch 1 or batches follow.
    None when absent."""
    plan = get_manufacturing_job(row).get("invention_bpc_plan")
    return plan if isinstance(plan, dict) else None


def get_invention_datacore_need_unknown(row: dict[str, Any]) -> str | None:
    """Why the producer could not size this row's datacores
    (manufacturing_job.invention_datacore_need_unknown.reason), or None."""
    unknown = get_manufacturing_job(row).get("invention_datacore_need_unknown")
    if not isinstance(unknown, dict):
        return None
    reason = unknown.get("reason")
    return str(reason) if reason else None


def get_invention_decryptor(row: dict[str, Any]) -> tuple[int, str] | None:
    """(type_id, name) of the decryptor the producer chose for this row's
    invention (manufacturing_job.invention_decryptor), or None for none."""
    choice = get_manufacturing_job(row).get("invention_decryptor")
    if not isinstance(choice, dict):
        return None
    type_id = _as_int(choice.get("type_id"))
    if type_id <= 0:
        return None
    return type_id, str(choice.get("type_name") or type_id)


def _non_negative_float(value: Any) -> float | None:
    """A fee: a finite number >= 0, else None (unknown, never 0)."""
    if isinstance(value, bool):
        return None
    number = _as_optional_float(value)
    if number is None or number != number or number < 0 or number == float("inf"):
        return None
    return number


def get_manufacturing_fee(row: dict[str, Any]) -> float | None:
    """The batch's manufacturing install fee (manufacturing_job_cost: EIV x
    cost index x structure/rig reductions + surcharge), or None when the
    producer could not price it (no cost index or no EIV)."""
    return _non_negative_float(get_manufacturing_job(row).get("manufacturing_job_cost"))


def get_manufacturing_fee_per_run(row: dict[str, Any]) -> float | None:
    """The manufacturing fee of one run. The fee is EIV-based (ME 0
    materials), so it is linear in runs and independent of ME. None when the
    fee or the runs are unknown."""
    fee = get_manufacturing_fee(row)
    runs = _as_int(get_manufacturing_job(row).get("runs"))
    return fee / runs if fee is not None and runs > 0 else None


def _invention_fee_rate(row: dict[str, Any], key: str) -> float | None:
    """A per-attempt / per-run invention fee rate from the row's own
    invention plan (activity_breakdown.invention), else from the later-batch
    invention_bpc_plan (batch 1 covered by an owned T2 BPC)."""
    for plan in (get_invention_costs(row), get_invention_bpc_plan(row)):
        if plan is not None and plan.get(key) is not None:
            return _non_negative_float(plan.get(key))
    return None


def get_invention_fee_per_attempt(row: dict[str, Any]) -> float | None:
    """The install fee of one invention attempt (job_cost_per_attempt), or None."""
    return _invention_fee_rate(row, "job_cost_per_attempt")


def get_source_copy_fee_per_run(row: dict[str, Any]) -> float | None:
    """The install fee of one copy run of the T1 source blueprint
    (source_copy_job_cost_per_run), or None."""
    return _invention_fee_rate(row, "source_copy_job_cost_per_run")


def get_research_fee(row: dict[str, Any], activity: str, from_level: int, to_level: int) -> float | None:
    """The install fee of researching the row's owned BPO from `from_level`
    to `to_level` (activity 'research_material' or 'research_time', levels
    0..10): the producer's cumulative fee to `to_level` minus that to
    `from_level` (manufacturing_job.research_job_cost_by_level). None
    when the table is absent or unpriced, or the levels are out of range."""
    tables = get_manufacturing_job(row).get("research_job_cost_by_level")
    table = tables.get(activity) if isinstance(tables, dict) else None
    if not isinstance(table, list) or not (0 <= from_level <= to_level < len(table)):
        return None
    high, low = _non_negative_float(table[to_level]), _non_negative_float(table[from_level])
    return high - low if high is not None and low is not None else None


def get_research_duration_seconds(
    row: dict[str, Any], activity: str, from_level: int, to_level: int, *, unskilled: bool = False,
    base: bool = False,
) -> int | None:
    """Seconds to research the row's owned BPO from `from_level` to
    `to_level` (levels 0..10): the producer's cumulative duration to
    `to_level` minus that to `from_level`
    (manufacturing_job.research_duration_seconds_by_level, with the
    producer character's research skills; `unskilled=True` reads the
    _unskilled table, without them; `base=True` the _base table, without
    skills or implants: structure and rigs only). None
    when the table is absent, or the levels are out of range."""
    key = "research_duration_seconds_by_level" + ("_base" if base else "_unskilled" if unskilled else "")
    tables = get_manufacturing_job(row).get(key)
    table = tables.get(activity) if isinstance(tables, dict) else None
    if not isinstance(table, list) or not (0 <= from_level <= to_level < len(table)):
        return None
    try:
        high, low = int(table[to_level]), int(table[from_level])
    except (TypeError, ValueError):
        return None
    return high - low if high >= low >= 0 else None


def get_research_implant_time_multiplier(row: dict[str, Any], character_id: int, activity: str) -> float | None:
    """The share of a research job's duration pilot `character_id`'s implants
    leave (manufacturing_job.research_implant_time_multiplier_by_character_id),
    or None when the producer published none for that pilot."""
    table = get_manufacturing_job(row).get("research_implant_time_multiplier_by_character_id")
    entry = table.get(str(int(character_id))) if isinstance(table, dict) else None
    value = entry.get(activity) if isinstance(entry, dict) else None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < float(value) <= 1:
        return None
    return float(value)


def _published_reason(value: Any) -> str | None:
    return str(value) if isinstance(value, str) and value.strip() else None


def get_manufacturing_fee_unknown_reason(row: dict[str, Any]) -> str | None:
    """Why the producer could not price the manufacturing fee (a missing
    index, an unpriced material, an unknown facility tax), or None when it
    published no reason: an older row, which a product
    overview refresh fixes."""
    return _published_reason(get_manufacturing_job(row).get("manufacturing_job_cost_unknown_reason"))


def get_invention_fee_unknown_reason(row: dict[str, Any], key: str) -> str | None:
    """Why the per-attempt (`job_cost_per_attempt`) or per-source-copy-run
    (`source_copy_job_cost_per_run`) fee is unknown, from the same plan the
    fee is read from; None = no published reason (refresh)."""
    for plan in (get_invention_costs(row), get_invention_bpc_plan(row)):
        if plan is None:
            continue
        reason = _published_reason(plan.get(f"{key}_unknown_reason"))
        if reason is not None:
            return reason
    return None


def get_research_fee_unknown_reason(row: dict[str, Any], activity: str) -> str | None:
    """Why the research fee table of `activity` is unknown; None = no
    published reason (refresh)."""
    reasons = get_manufacturing_job(row).get("research_job_cost_by_level_unknown_reason")
    return _published_reason(reasons.get(activity)) if isinstance(reasons, dict) else None


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
