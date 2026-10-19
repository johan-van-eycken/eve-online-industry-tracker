"""Research first (Task 8c fix rounds 1-2, the user's decision).

EVE runs one job per blueprint: a BPO in ME/TE research can neither build
nor be copied. When ChainPlanner schedules research on an owned BPO whose
batch builds from or copies it, the item falls back, in this order, to:

1. an owned-BPC variant row of the same product (the producer's row that
   builds from the corp's BPCs, which dedupe_by_type_id dropped): built in
   full, with that row's own segments, cost and profit;
2. the owned-BPC runs of a batch (the producer's owned_blueprint_copy job
   segments): only those runs are built, with their own materials; the
   producer priced the whole batch, so material cost and profit are unknown;
3. nothing: the item is not built in this plan.

With several BPOs of the type, another free BPO builds instead: from its own
producer row, else sized by the planner from the SDE at its ME in the row's
structure (Task 8c fix round 5). A BPO installed in an active industry job
is unavailable the same way, but unconditionally: it is not waiting for a
research slot.

ChainPlanner decides it once, before the sub-components are sized, and
records it on the row as `research_first`; the sub-component requests and
CharacterAssigner's manufacture action both follow that one decision.
"""
from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.domain import industry_activities as _activities

#: Batch sources that need the owned BPO itself: building from it, or copying it.
BPO_SOURCES = frozenset({"owned_blueprint_original", "copied_from_owned_blueprint_original"})
OWNED_BPC_SOURCE = "owned_blueprint_copy"

#: The row keys ChainPlanner's research scheduling writes.
RESEARCH_KEYS = (
    "needs_me_research", "me_research_target", "me_current",
    "needs_te_research", "te_research_target", "te_current",
    # The plan ledger's label for the research claim (Task 14 M10).
    "research_claim_label",
)

PART_COST_UNKNOWN_REASON = (
    "the producer priced the whole batch, not its owned-BPC runs: material cost and profit unknown"
)


def job_segments(row: dict[str, Any]) -> list[dict[str, Any]] | None:
    segments = orow.get_manufacturing_job(row).get("job_segments")
    if not isinstance(segments, list) or not all(isinstance(s, dict) for s in segments):
        return None
    return segments


def batch_uses_bpo(row: dict[str, Any]) -> bool:
    """Whether the batch builds from, or copies, the owned BPO. Without job
    segments the batch's source kind decides; with neither the BPO is
    assumed in use (the chain planner chose the BPO path)."""
    segments = job_segments(row)
    if segments:
        return any(str(s.get("source")) in BPO_SOURCES for s in segments)
    kind = orow.get_manufacturing_job(row).get("blueprint_source_kind")
    return kind is None or str(kind) in BPO_SOURCES


def owned_bpc_part(row: dict[str, Any], *, exclude_item_ids: Any = frozenset()) -> dict[str, Any] | None:
    """The part of the batch on owned BPCs: {runs, quantity, materials,
    item_ids}, or None when there is none or its materials are unknown. Jobs on a BPC in `exclude_item_ids` (one in an active job, or
    claimed by another job today) are left out.

    quantity is rounded UP (Task 14 minor 5): ceil(units x runs / total
    runs). The batch's other part is units - quantity, so the two add up
    to the batch exactly. item_ids are the BPCs the part
    runs on; None when a segment does not name its BPC (then any of the
    blueprint's BPCs may be the one)."""
    segments = [
        s for s in (job_segments(row) or [])
        if str(s.get("source")) == OWNED_BPC_SOURCE and segment_bpc_item_id(s) not in exclude_item_ids
    ]
    runs = sum(int(s.get("runs") or 0) for s in segments)
    if runs <= 0:
        return None
    materials: dict[int, int] = {}
    for segment in segments:
        per_job = segment.get("materials")
        if not isinstance(per_job, dict):
            return None
        for type_id, quantity in per_job.items():
            try:
                materials[int(type_id)] = materials.get(int(type_id), 0) + int(quantity)
            except (TypeError, ValueError):
                return None
    total_runs = int(orow.get_manufacturing_job(row).get("runs") or 0)
    units = orow.get_product_quantity(row)
    quantity = -(-(units * runs) // total_runs) if total_runs > 0 and units > 0 else None
    named = [segment_bpc_item_id(s) for s in segments]
    return {
        "runs": runs, "quantity": quantity,
        "materials": materials,
        "item_ids": None if any(i is None for i in named) else sorted(set(named)),
    }


def segment_bpc_item_id(segment: dict[str, Any]) -> int | None:
    """The owned BPC a job segment runs on (the producer's
    blueprint_item_id), or None when the segment does not say."""
    try:
        value = int(segment.get("blueprint_item_id") or 0)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def builds_on_owned_bpcs(row: dict[str, Any]) -> bool:
    """Whether some of the batch's runs come from owned BPCs."""
    segments = job_segments(row)
    if segments:
        return any(str(s.get("source")) == OWNED_BPC_SOURCE for s in segments)
    return orow.get_manufacturing_job(row).get("blueprint_source_kind") == OWNED_BPC_SOURCE


def row_bpc_item_ids(row: dict[str, Any]) -> set[int]:
    """The item_ids of the owned BPCs the producer planned the row on
    (manufacturing_job.blueprint_copy: `item_id` for one BPC, `item_ids` for
    several). Empty when it names none."""
    payload = orow.get_manufacturing_job(row).get("blueprint_copy")
    if not isinstance(payload, dict):
        return set()
    raw = [payload.get("item_id"), *(payload.get("item_ids") or [])]
    out: set[int] = set()
    for value in raw:
        try:
            item = int(value) if value is not None else 0
        except (TypeError, ValueError):
            continue
        if item > 0:
            out.add(item)
    return out


def _job_field(job: Any, name: str) -> Any:
    return job.get(name) if isinstance(job, dict) else getattr(job, name, None)


#: Activities that only a BPO can run: ME and TE research, copying.
_BPO_ONLY_ACTIVITY_IDS = frozenset({_activities.ACTIVITY_ME_RESEARCH, _activities.ACTIVITY_TE_RESEARCH,
                                    _activities.ACTIVITY_COPYING})
#: Activities that only a BPC can run: invention.
_BPC_ONLY_ACTIVITY_IDS = frozenset({_activities.ACTIVITY_INVENTION})


def busy_blueprint_type_ids(
    industry_jobs: list[Any], *, copies: bool, is_copy_by_item_id: dict[int, bool] | None = None,
) -> set[int]:
    """Blueprint type ids with a BPO (copies=False) or a BPC (copies=True)
    installed in one of these active jobs.

    Whether a job's blueprint is a copy: the job's own flag; when that is
    NULL (as on many live rows), the item's flag from the asset snapshot or
    history (`is_copy_by_item_id`); else the activity (research and copying
    need a BPO, invention a BPC). A job none of these resolves (e.g.
    manufacturing) counts as both, so a busy BPO is never missed."""
    out: set[int] = set()
    for job in industry_jobs or []:
        try:
            bp_type_id = int(_job_field(job, "blueprint_type_id") or 0)
            activity_id = int(_job_field(job, "activity_id") or 0)
            item = int(_job_field(job, "blueprint_item_id") or 0)
        except (TypeError, ValueError):
            continue
        if bp_type_id <= 0:
            continue
        is_copy = _job_field(job, "blueprint_is_blueprint_copy")
        if is_copy is None and item > 0:
            is_copy = (is_copy_by_item_id or {}).get(item)
        if is_copy is None:
            if activity_id in _BPO_ONLY_ACTIVITY_IDS:
                is_copy = False
            elif activity_id in _BPC_ONLY_ACTIVITY_IDS:
                is_copy = True
        if is_copy is None or bool(is_copy) == copies:
            out.add(bp_type_id)
    return out


def best_bpc_variant(variants: list[dict[str, Any]], blueprint_type_id: int) -> dict[str, Any] | None:
    """The first variant row of the same blueprint that does not touch the
    BPO and builds on owned BPCs, or None."""
    for variant in variants or []:
        if not isinstance(variant, dict) or orow.get_blueprint_type_id(variant) != blueprint_type_id:
            continue
        if batch_uses_bpo(variant):
            continue
        segments = job_segments(variant)
        on_bpcs = (
            any(str(s.get("source")) == OWNED_BPC_SOURCE for s in segments) if segments
            else orow.get_manufacturing_job(variant).get("blueprint_source_kind") == OWNED_BPC_SOURCE
        )
        if on_bpcs:
            return variant
    return None


def best_part(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The owned-BPC part with the most runs over these rows, or None."""
    parts = [p for p in (owned_bpc_part(r) for r in rows) if p is not None]
    return max(parts, key=lambda p: p["runs"]) if parts else None


def _level(asset: Any, attr: str) -> int | None:
    value = asset.get(attr) if isinstance(asset, dict) else getattr(asset, attr, None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def item_id(asset: Any) -> int | None:
    """The asset's item_id, or None."""
    return _level(asset, "item_id")


def bpo_levels(asset: Any) -> tuple[int | None, int | None]:
    """(ME, TE) of an owned BPO asset; None = unknown."""
    return _level(asset, "blueprint_material_efficiency"), _level(asset, "blueprint_time_efficiency")


def describe_bpo(asset: Any) -> str:
    me, te = bpo_levels(asset)
    return f"ME {me if me is not None else '?'} / TE {te if te is not None else '?'}"


def producer_bpo_order(assets: list[Any]) -> list[Any]:
    """The owned BPOs in the producer's order (IndustryService sorts
    blueprint originals by ME, then TE, highest first, then item_id), so the
    planner's choice does not depend on the asset order."""
    return sorted(assets, key=lambda a: (
        -(bpo_levels(a)[0] or 0), -(bpo_levels(a)[1] or 0), _level(a, "item_id") or 0,
    ))


def _row_bpo_item_id(row: dict[str, Any]) -> int | None:
    original = orow.get_manufacturing_job(row).get("blueprint_original")
    return _level(original, "item_id") if isinstance(original, dict) else None


def priced_bpo_index(row: dict[str, Any], assets: list[Any]) -> int:
    """Index (in producer order) of the BPO the row is priced from: the
    row's blueprint_original item, else the producer's first BPO."""
    item_id = _row_bpo_item_id(row)
    if item_id is not None:
        for index, asset in enumerate(assets):
            if _level(asset, "item_id") == item_id:
                return index
    return 0


def variant_for_bpo(variants: list[dict[str, Any]], blueprint_type_id: int, asset: Any) -> dict[str, Any] | None:
    """The producer's variant row priced from this BPO (its
    blueprint_original item), or None."""
    item_id = _level(asset, "item_id")
    if item_id is None:
        return None
    for variant in variants or []:
        if (isinstance(variant, dict) and orow.get_blueprint_type_id(variant) == blueprint_type_id
                and _row_bpo_item_id(variant) == item_id):
            return variant
    return None
