from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, cast

logger = logging.getLogger(__name__)

#: Stamped by the producer on every build_from_bpc row
#: (manufacturing_job.later_batch_plan_version) since batches 2..N got their
#: own plans (extra_batch_schedule). A row without it comes from an older
#: overview whose later batches repeat batch 1.
#: 2: rows also publish the nested and top-level BPC
#: runs batch 1 spent (shared-BPC netting); a version-1 row lacks them.
LATER_BATCH_PLAN_VERSION = 2

#: The material lists the shopping list reads, shared with the producer
#: (IndustryService._buy_line_type_ids publishes the stock of exactly these
#: lines' types). A row's own lists: batch 1's manufacturing and invention
#: lists, then the per-later-batch lists of a row without a schedule.
PROCUREMENT_KEY = "procurement_materials"
INVENTION_KEY = "invention_procurement_materials"
EXTRA_BATCH_INVENTION_KEY = "invention_procurement_materials_per_extra_batch"
NESTED_EXTRA_BATCH_INVENTION_KEY = "invention_procurement_materials_per_extra_batch_nested"
ROW_BUY_LIST_KEYS = (PROCUREMENT_KEY, INVENTION_KEY, EXTRA_BATCH_INVENTION_KEY, NESTED_EXTRA_BATCH_INVENTION_KEY)
#: One extra_batch_schedule plan's lists.
PLAN_NESTED_INVENTION_KEY = "invention_procurement_materials_nested"
LATER_BATCH_PLAN_BUY_LIST_KEYS = (PROCUREMENT_KEY, INVENTION_KEY, PLAN_NESTED_INVENTION_KEY)
#: The row's own invention over all batches, and its per-attempt inputs.
BPC_PLAN_KEY = "invention_bpc_plan"
BPC_PLAN_LINES_KEY = "per_attempt_materials"
EXTRA_BATCH_SCHEDULE_KEY = "extra_batch_schedule"
#: The nested inventions of batch 1 (row) and of each later plan, and the
#: owned BPC runs each nested blueprint has left after batch 1.
NESTED_BPC_PLANS_KEY = "nested_invention_bpc_plans"
NESTED_RUNS_LEFT_KEY = "nested_blueprint_runs_left"
#: The nested BPC runs batch 1 spent, and the owned runs
#: it was planned against (each row plans on its own copy of the pool).
NESTED_RUNS_USED_KEY = "nested_blueprint_runs_used"
NESTED_RUNS_OWNED_KEY = "nested_blueprint_runs_owned"
#: manufacturing_job.owned_stock_by_type_id: {str(type_id): quantity}, the
#: hangar stock the producer planned the row against, for its buy-line types,
#: and owned_stock_pool_id: one id per compute (a hash of that stock).
OWNED_STOCK_KEY = "owned_stock_by_type_id"
OWNED_STOCK_POOL_KEY = "owned_stock_pool_id"


def aggregate_shopping_list(selected_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The shopping list alone; see ``aggregate_shopping_list_with_stale``."""
    return aggregate_shopping_list_with_stale(selected_rows)[0]


def aggregate_shopping_list_with_stale(
    selected_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    """Aggregate procurement materials from selected overview rows into a shopping list.

    Each material line is ONE batch's take-or-buy plan: ``quantity`` needed, of
    which the producer takes ``take_quantity`` from owned stock and buys
    ``buy_quantity``. Owned stock exists once, so it is not scaled by batches:

        need = sum over rows of (quantity per batch x that row's batches)
        buy  = max(0, need - owned)

    Batches 2..N need not repeat batch 1: batch 1 may run on owned BPC runs
    (top level or in a sub-build) that later batches no longer have, and a
    later batch may be split over two invented BPCs (one job and one ME
    rounding each). Such a row carries ``extra_batch_schedule``: one plan per
    distinct later batch, and which plan each later batch uses. The need is
    then batch 1 plus each later batch's own plan (see
    ``_row_lines_with_batch_counts``).

    ``owned`` per type_id is the hangar stock the producer planned the rows
    against (``manufacturing_job.owned_stock_by_type_id``). Every row of one
    overview is planned against a copy of the same stock, so the aggregated
    need (batch 1's takes included) is netted against it exactly once. Rows
    from different refreshes (different ``owned_stock_pool_id``) net the
    smallest published figure; ``rows_from_different_stock_pools`` flags them. A row
    without that key (an overview cached before it existed) falls back to what
    it took: within a row the lines spend the stock in turn, so a row's summed
    take is stock the producer saw. ``owned`` is the largest of these figures,
    never more than the stock the producer reported, so never an under-buy.

    Gaps in the data never make the list short:
    - A line without an explicit ``buy_quantity`` counts nothing as owned and
      buys its full quantity. A "take" / "split" line without the split comes
      from an overview cached before the producer wrote it; those types are
      named in one DEBUG line per call (the UI shows a caption for them).
      That gap over-buys.
    - A row without ``owned_stock_by_type_id`` may hold more stock than it
      took: the take bound then over-buys. Its types are named in the stale
      map too, unless a selected row published the stock for that type or
      some row's batch 1 bought some of it (that take emptied the stock, and
      owned is at least that take, so it is exact).
    - A build_from_bpc row without ``later_batch_plan_version`` comes from an
      overview older than the later-batch plans: its later batches repeat
      batch 1, which can under-buy. Such a row contributes NOTHING to the
      list; ``rows_built_by_an_older_version`` names it so the UI can ask for
      a refresh before buying.

    Returns (items, stale) where items are dicts sorted by (buy * unit_price)
    descending:
        {"type_id": int, "type_name": str, "need": int, "buy": int, "unit_price": float | None}
    and stale maps the type_id of every such split-less or take-bound line to
    its name, so a UI can tell the user to refresh. ``unit_price`` is the first known price.
    """
    accumulated: dict[int, dict[str, Any]] = {}
    owned_by_type_id: dict[int, int] = {}
    # Per type, every published figure: one per stock pool (refresh).
    published_figures_by_type_id: dict[int, list[int]] = {}
    emptied_type_ids: set[int] = set()
    stale_type_names: dict[int, str] = {}
    # Keyless rows' types whose take may understate the stock (see above).
    take_bound_type_names: dict[int, str] = {}

    # The same row selected twice is one row.
    selected_rows = _distinct_rows(selected_rows)
    selected_jobs = [
        mj for mj in (row.get("manufacturing_job") if isinstance(row, dict) else None for row in selected_rows)
        if isinstance(mj, dict)
    ]
    counted_rows = [
        (row["manufacturing_job"], max(1, int(row.get("max_batches_total") or 1))) for row in selected_rows
        if isinstance(row, dict) and isinstance(row.get("manufacturing_job"), dict)
        and not _row_predates_later_batch_plans(row["manufacturing_job"])
    ]
    # Every selected row, a refused older one included: it may still be built
    # from the same BPCs, so it also stops a spare-run credit.
    blueprint_users = [_blueprints_whose_bpc_runs_it_may_use(mj) for mj in selected_jobs]
    # BPC runs several rows spend: sized once over rows and batches.
    shared = _shared_bpc_plan(counted_rows)
    shared_excess_lines = shared.lines

    for row in selected_rows:
        if not isinstance(row, dict):
            continue
        mj = row.get("manufacturing_job")
        if not isinstance(mj, dict):
            continue
        if _row_predates_later_batch_plans(mj):
            # Possibly short: never counted (rows_built_by_an_older_version).
            continue
        batches = max(1, int(row.get("max_batches_total") or 1))
        others = [used for job, used in zip(selected_jobs, blueprint_users) if job is not mj]
        lines = _row_lines_with_batch_counts(
            mj, batches,
            skip_nested_blueprints=shared.skip_nested.get(id(mj), frozenset()),
            skip_own_invention=id(mj) in shared.skip_own,
            # Owned spare runs are this row's alone only when no other
            # selected row may build from that blueprint's BPCs.
            exclusive_blueprint=lambda bp, _others=others: all(
                used is not None and bp not in used for used in _others
            ),
        )
        if not lines:
            continue

        row_take_by_type_id: dict[int, int] = {}
        stock = mj.get(OWNED_STOCK_KEY)
        row_published: set[int] = set()
        for type_key, quantity in (stock.items() if isinstance(stock, dict) else []):
            parsed = _published_quantity(type_key, quantity)
            if parsed is not None:
                row_published.add(parsed[0])
                published_figures_by_type_id.setdefault(parsed[0], []).append(parsed[1])
        emptied_type_ids |= _batch_one_emptied_type_ids(mj)

        for mat, line_batches in lines:
            if not isinstance(mat, dict):
                continue
            try:
                mat_type_id = int(mat["type_id"])
            except (KeyError, TypeError, ValueError):
                continue

            quantity_per_batch = int(mat.get("quantity") or 0)
            if "buy_quantity" not in mat and str(mat.get("sourcing_strategy") or "").lower() in _OWNED_STRATEGIES:
                stale_type_names.setdefault(mat_type_id, str(mat.get("type_name") or mat_type_id))
            if mat_type_id not in row_published:
                take_bound_type_names.setdefault(mat_type_id, str(mat.get("type_name") or mat_type_id))
            take_per_batch = max(0, min(quantity_per_batch, quantity_per_batch - _buy_per_batch(mat, quantity_per_batch)))
            row_take_by_type_id[mat_type_id] = row_take_by_type_id.get(mat_type_id, 0) + take_per_batch

            need_total = quantity_per_batch * line_batches
            if mat_type_id in accumulated:
                accumulated[mat_type_id]["need"] += need_total
            else:
                accumulated[mat_type_id] = {
                    "type_id": mat_type_id,
                    "type_name": str(mat.get("type_name") or mat_type_id),
                    "need": need_total,
                    "buy": 0,
                    "unit_price": _safe_float(mat.get("unit_price")),
                }
            if accumulated[mat_type_id]["unit_price"] is None:
                accumulated[mat_type_id]["unit_price"] = _safe_float(mat.get("unit_price"))

        for mat_type_id, row_take in row_take_by_type_id.items():
            owned_by_type_id[mat_type_id] = max(owned_by_type_id.get(mat_type_id, 0), row_take)

    for mat in shared_excess_lines:
        mat_type_id = int(mat["type_id"])
        if mat_type_id in accumulated:
            accumulated[mat_type_id]["need"] += int(mat["quantity"])
        else:
            accumulated[mat_type_id] = {
                "type_id": mat_type_id, "type_name": str(mat.get("type_name") or mat_type_id),
                "need": int(mat["quantity"]), "buy": 0, "unit_price": _safe_float(mat.get("unit_price")),
            }

    # One pool: every figure is the same stock. Rows from different refreshes
    # (rows_from_different_stock_pools): the smallest, which every one of
    # them saw. Never below a row's take.
    published_stock_by_type_id = {
        type_id: min(figures) for type_id, figures in published_figures_by_type_id.items()
    }
    for mat_type_id, item in accumulated.items():
        owned = max(owned_by_type_id.get(mat_type_id, 0), published_stock_by_type_id.get(mat_type_id, 0))
        item["buy"] = max(0, item["need"] - owned)
    for mat_type_id, name in take_bound_type_names.items():
        # Exact when any row published the type, or any row's batch 1 bought
        # some (that row's take is the whole stock, and owned >= that take).
        if mat_type_id not in published_stock_by_type_id and mat_type_id not in emptied_type_ids:
            stale_type_names.setdefault(mat_type_id, name)

    if stale_type_names:
        # DEBUG: this runs on every UI rerun, and the UI shows the stale
        # caption from the returned names, which is the real signal.
        logger.debug(
            "Shopping list: %s came from an older overview (no take/buy split, or no published hangar "
            "stock); counted conservatively (may over-buy). Refresh the Industry Builder overview for exact stock use.",
            ", ".join(sorted(stale_type_names.values())),
        )

    result = list(accumulated.values())
    result.sort(key=lambda r: (r["buy"] * (r["unit_price"] or 0.0)), reverse=True)
    return result, stale_type_names


DIFFERENT_REFRESHES_WARNING = "Rows from different refreshes \u2014 refresh before buying."


def rows_from_different_stock_pools(selected_rows: list[dict[str, Any]]) -> bool:
    """True when the selected rows published stock from more than one pool.

    Every row of one overview compute carries the same owned_stock_pool_id.
    Rows from different refreshes may have seen different hangar stock; the
    list then nets the smallest published figure per type (never more than
    any of them saw), and the UI warns with DIFFERENT_REFRESHES_WARNING. A
    row with stock but no pool id counts as a pool of its own.
    """
    pools: set[Any] = set()
    for row in selected_rows:
        mj = row.get("manufacturing_job") if isinstance(row, dict) else None
        if isinstance(mj, dict) and isinstance(mj.get(OWNED_STOCK_KEY), dict):
            pools.add(mj.get(OWNED_STOCK_POOL_KEY) or ("no pool id", id(mj)))
    return len(pools) > 1


OLDER_OVERVIEW_REASON = "this overview was built by an older version; refresh it before buying"


def rows_built_by_an_older_version(selected_rows: list[dict[str, Any]]) -> dict[str, str]:
    """The selected rows the shopping list leaves out, by row name, with why.

    A build_from_bpc row without ``later_batch_plan_version`` was planned
    before batches 2..N got their own plans, so its later batches may be short
    (an owned BPC's runs spent, or a batch split over two invented BPCs). The
    list shows none of its quantities; the UI warns with these names. Logs at
    DEBUG only (it runs on every UI rerun; the UI warning is the signal).
    """
    out: dict[str, str] = {}
    for row in selected_rows:
        if not isinstance(row, dict):
            continue
        mj = row.get("manufacturing_job")
        if isinstance(mj, dict) and _row_predates_later_batch_plans(mj):
            out[str(row.get("type_name") or row.get("type_id") or "?")] = OLDER_OVERVIEW_REASON
    if out:
        logger.debug("Shopping list: left out %s (%s)", ", ".join(sorted(out)), OLDER_OVERVIEW_REASON)
    return out


def invention_need_unknown_rows(selected_rows: list[dict[str, Any]]) -> dict[str, str]:
    """Rows whose datacore need the shopping list cannot size, by row name.

    The producer writes ``manufacturing_job.invention_datacore_need_unknown``
    = {"reason", "from_batch"} when invention odds are unknown for batches it
    must invent: from batch 1, or from batch 2 when an owned BPC covers batch 1
    only. Those datacores are in no invention list, so the shopping list would
    show 0 for them; a row is named here when its batches reach that batch,
    so the UI can flag the gap instead. Never guesses a quantity. Logs at
    DEBUG only (see aggregate_shopping_list_with_stale).
    """
    out: dict[str, str] = {}
    for row in selected_rows:
        if not isinstance(row, dict):
            continue
        mj = row.get("manufacturing_job")
        unknown = mj.get(_UNKNOWN_DATACORE_NEED_KEY) if isinstance(mj, dict) else None
        if not isinstance(unknown, dict):
            continue
        batches = max(1, int(row.get("max_batches_total") or 1))
        if batches < max(1, int(unknown.get("from_batch") or 1)):
            continue
        name = str(row.get("type_name") or row.get("type_id") or "?")
        out[name] = str(unknown.get("reason") or "invention odds unknown")
    if out:
        # DEBUG: the producer already logged one WARNING per row, this runs on
        # every UI rerun, and the UI's st.warning is the real signal.
        logger.debug(
            "Shopping list: datacore need unknown for %s; no datacores counted for those batches",
            ", ".join(sorted(out)),
        )
    return out


_OWNED_STRATEGIES = frozenset({"take", "split", "mixed"})

_UNKNOWN_DATACORE_NEED_KEY = "invention_datacore_need_unknown"



def _row_lines_with_batch_counts(
    mj: dict[str, Any], batches: int, *, exclusive_blueprint: Any = None,
    skip_nested_blueprints: Any = frozenset(), skip_own_invention: bool = False,
) -> list[tuple[Any, int]]:
    """Each material line of a row with the number of batches it is needed for.

    procurement_materials: batch 1's manufacturing buy list.
    invention_procurement_materials: the invention inputs (whole attempts) for
    batch 1, after the owned T2 BPC runs; absent when they cover batch 1.

    Batches 2..N, with an ``extra_batch_schedule`` (the producer writes it
    when a later batch differs from batch 1): each later batch takes its own
    plan's ``procurement_materials`` and invention inputs (see
    ``_later_batch_plan_counts``). Without it every later batch repeats batch
    1's procurement_materials.

    The row's own invention for batches 2..N, with an ``invention_bpc_plan``:
    later batches share invented BPC runs, so it is sized over all
    batches (``_later_batch_invention_lines``), and only the nested
    sub-builds' inputs come per batch (a plan's
    ``invention_procurement_materials_nested``, or the row's
    ``..._per_extra_batch_nested`` list without a schedule). Without the
    plan (unknown odds, or an older overview): one full later batch's inputs
    per later batch (a plan's ``invention_procurement_materials``, or the
    row's invention_procurement_materials_per_extra_batch). An overview cached
    before that list existed lacks the key, and batch 1's invention list then
    counts for every batch. Every later-batch line takes nothing from stock.

    Nested inventions, on a row that publishes ``nested_invention_bpc_plans``:
    their later-batch inputs are not taken per batch but sized over all
    batches (``_later_batch_nested_invention_lines``), like the row's own.
    `exclusive_blueprint(bp)` says whether the row may count blueprint bp's
    owned BPC runs left after batch 1 as its own. `skip_nested_blueprints` /
    `skip_own_invention`: blueprints whose later-batch invention
    ``_shared_bpc_plan`` sizes over all selected rows instead.
    """
    def lines(source: dict[str, Any], key: str) -> list[Any]:
        value = source.get(key)
        return list(value.values()) if isinstance(value, dict) else []

    plan = mj.get(BPC_PLAN_KEY)
    has_bpc_plan = isinstance(plan, dict)
    nested_model = isinstance(mj.get(NESTED_BPC_PLANS_KEY), list)
    # Later batches' nested inventions (count, plan list) for the I3 sizing.
    later_nested: list[tuple[int, Any]] = []
    schedule = mj.get(EXTRA_BATCH_SCHEDULE_KEY)
    if schedule is not None:
        out: list[tuple[Any, int]] = [(mat, 1) for mat in lines(mj, PROCUREMENT_KEY)]
        out.extend((mat, 1) for mat in lines(mj, INVENTION_KEY))
        plans = schedule.get("plans") if isinstance(schedule, dict) else None
        invention_key = PLAN_NESTED_INVENTION_KEY if has_bpc_plan else INVENTION_KEY
        for plan_index, count in _later_batch_plan_counts(schedule, batches - 1).items():
            later_plan = cast(list[dict[str, Any]], plans)[plan_index]
            out.extend((mat, count) for mat in lines(later_plan, PROCUREMENT_KEY))
            if not nested_model:
                out.extend((mat, count) for mat in lines(later_plan, invention_key))
                continue
            if not has_bpc_plan:
                # The row's own invention for this batch alone: the plan's
                # list minus its nested part.
                out.extend((mat, count) for mat in _minus(
                    lines(later_plan, INVENTION_KEY), lines(later_plan, PLAN_NESTED_INVENTION_KEY),
                ))
            later_nested.append((count, later_plan.get(NESTED_BPC_PLANS_KEY)))
        if has_bpc_plan and batches > 1 and not skip_own_invention:
            out.extend((mat, 1) for mat in _later_batch_invention_lines(cast(dict[str, Any], plan), batches))
    else:
        out = [(mat, batches) for mat in lines(mj, PROCUREMENT_KEY)]
        if has_bpc_plan:
            out.extend((mat, 1) for mat in lines(mj, INVENTION_KEY))
            if batches > 1:
                if not skip_own_invention:
                    out.extend((mat, 1) for mat in _later_batch_invention_lines(cast(dict[str, Any], plan), batches))
                if not nested_model:
                    out.extend((mat, batches - 1) for mat in lines(mj, NESTED_EXTRA_BATCH_INVENTION_KEY))
        elif isinstance(mj.get(EXTRA_BATCH_INVENTION_KEY), dict):
            out.extend((mat, 1) for mat in lines(mj, INVENTION_KEY))
            if batches > 1:
                extra = lines(mj, EXTRA_BATCH_INVENTION_KEY)
                if nested_model:
                    extra = _minus(extra, lines(mj, NESTED_EXTRA_BATCH_INVENTION_KEY))
                out.extend((mat, batches - 1) for mat in extra)
        else:
            out.extend((mat, batches) for mat in lines(mj, INVENTION_KEY))
        if batches > 1:
            # Every later batch repeats batch 1, nested inventions included.
            later_nested.append((batches - 1, mj.get(NESTED_BPC_PLANS_KEY)))
    if nested_model and batches > 1:
        out.extend((mat, 1) for mat in _later_batch_nested_invention_lines(
            mj, later_nested, exclusive_blueprint or (lambda bp: False), skip=skip_nested_blueprints,
        ))
    return out


def _int_map(value: Any) -> dict[int, int]:
    out: dict[int, int] = {}
    for key, runs in (value.items() if isinstance(value, dict) else []):
        try:
            out[int(key)] = max(0, int(runs or 0))
        except (TypeError, ValueError):
            continue
    return out


@dataclass
class _SharedBpcPlan:
    """What ``_shared_bpc_plan`` sizes: the lines to add once, the rows'
    later-batch invention it replaces (nested blueprints per row id; rows
    whose own invention it replaces), and the shortfalls it cannot size
    ({blueprint: (runs short in batch 1, the rows)})."""
    lines: list[dict[str, Any]] = field(default_factory=list)
    skip_nested: dict[int, set[int]] = field(default_factory=dict)
    skip_own: set[int] = field(default_factory=set)
    unsized: dict[int, tuple[int, list[dict[str, Any]]]] = field(default_factory=dict)


def _distinct_rows(rows: list[Any]) -> list[Any]:
    """`rows` with any row selected more than once kept once (by identity)."""
    seen: set[int] = set()
    out: list[Any] = []
    for row in rows:
        if id(row) not in seen:
            seen.add(id(row))
            out.append(row)
    return out


def _nested_plan_lists(mj: dict[str, Any]) -> list[tuple[int | None, Any]]:
    """(plan index or None for batch 1, that plan's nested invention list)."""
    schedule = mj.get(EXTRA_BATCH_SCHEDULE_KEY)
    later_plans = schedule.get("plans") if isinstance(schedule, dict) else None
    return [(None, mj.get(NESTED_BPC_PLANS_KEY))] + [
        (index, plan.get(NESTED_BPC_PLANS_KEY)) for index, plan in enumerate(later_plans or [])
        if isinstance(plan, dict)
    ]


def _entries_for(plans: Any, bp: int) -> list[dict[str, Any]]:
    return [
        e for e in (plans or []) if isinstance(e, dict)
        and e.get("invented_blueprint_type_id") is not None and int(e["invented_blueprint_type_id"]) == bp
    ]


def _shared_bpc_plan(rows: list[tuple[dict[str, Any], int]]) -> _SharedBpcPlan:
    """BPC runs that several selected rows spend, sized once over all of
    them and all their batches.

    Every row is planned against its own copy of the owned BPC runs, so two
    rows can both spend the same owned run: two parents of one component,
    or a component row and a parent that nests it. Per blueprint spent in
    batch 1 by two or more rows (``nested_blueprint_runs_used``, the row's
    own top-level runs included):

        runs needed = sum over the rows of: a nested user's batch-1 runs on
        the BPC + its batch-1 invented runs + its later batches' runs (each
        later plan assumes no owned run left); a top-level user's batches x
        batch runs
        BPCs = ceil(max(0, runs needed - owned runs) / runs per BPC)
        attempts = BPCs x whole attempts per BPC

    The rows' batch-1 invention lines are kept, so only the attempts beyond
    them are added; each row's later-batch invention of that blueprint is
    left to this plan (never less than the runs needed, so no under-buy).
    A blueprint with no invention model in any of the rows, or a top-level
    user without an invention_bpc_plan, is not sized: ``unsized`` names it
    when batch 1 alone spends more than is owned.
    """
    out = _SharedBpcPlan()
    used_by: dict[int, list[tuple[dict[str, Any], int, int]]] = {}
    for mj, batches in rows:
        for bp, runs in _int_map(mj.get(NESTED_RUNS_USED_KEY)).items():
            if runs > 0:
                used_by.setdefault(bp, []).append((mj, batches, runs))
    for bp, users in used_by.items():
        if len(users) < 2:
            continue
        owned = min(_int_map(mj.get(NESTED_RUNS_OWNED_KEY)).get(bp, 0) for mj, _b, _r in users)
        runs_needed = 0
        bought_attempts = 0
        model: dict[str, Any] | None = None
        sizable = True
        for mj, batches, used in users:
            own_bp = (mj.get("blueprint_sde") or {}).get("blueprint_type_id") if isinstance(mj.get("blueprint_sde"), dict) else None
            if own_bp is not None and int(own_bp) == bp:
                plan = mj.get(BPC_PLAN_KEY)
                if not isinstance(plan, dict):
                    sizable = False
                    continue
                runs_needed += batches * max(0, int(plan.get("batch_runs") or 0))
                bought_attempts += max(0, int(plan.get("batch_one_whole_attempts") or 0))
                model = model or plan
                continue
            runs_needed += used
            schedule = mj.get(EXTRA_BATCH_SCHEDULE_KEY)
            counts = _later_batch_plan_counts(schedule, batches - 1) if schedule is not None and batches > 1 else {}
            for index, plans in _nested_plan_lists(mj):
                for entry in _entries_for(plans, bp):
                    model = model or entry
                    n = max(0, int(entry.get("invented_runs") or 0))
                    if index is None:
                        runs_needed += n
                        bought_attempts += max(0, int(entry.get("whole_attempts") or 0))
                    else:
                        runs_needed += counts.get(index, 0) * n
            if schedule is None and batches > 1:
                # Later batches repeat batch 1 (no owned run left to them).
                runs_needed += (batches - 1) * (used + sum(
                    max(0, int(e.get("invented_runs") or 0)) for e in _entries_for(mj.get(NESTED_BPC_PLANS_KEY), bp)
                ))
        per_attempt = (model or {}).get(BPC_PLAN_LINES_KEY)
        per_bpc = max(1, int((model or {}).get("runs_per_invented_blueprint") or 1))
        attempts_per_bpc = int((model or {}).get("whole_attempts_per_blueprint") or 0)
        if not sizable or attempts_per_bpc <= 0 or not isinstance(per_attempt, dict):
            short = sum(used for _mj, _b, used in users) - owned
            if short > 0:
                out.unsized[bp] = (short, [mj for mj, _b, _r in users])
            continue
        for mj, _batches, _used in users:
            own_bp = (mj.get("blueprint_sde") or {}).get("blueprint_type_id") if isinstance(mj.get("blueprint_sde"), dict) else None
            if own_bp is not None and int(own_bp) == bp:
                out.skip_own.add(id(mj))
            else:
                out.skip_nested.setdefault(id(mj), set()).add(bp)
        bpcs = -(-max(0, runs_needed - owned) // per_bpc)
        extra_attempts = max(0, bpcs * attempts_per_bpc - bought_attempts)
        for mat in per_attempt.values():
            if isinstance(mat, dict) and int(mat.get("quantity") or 0) > 0 and mat.get("type_id") is not None:
                quantity = int(mat["quantity"]) * extra_attempts
                if quantity > 0:
                    out.lines.append({**mat, "quantity": quantity, "take_quantity": 0, "buy_quantity": quantity})
    return out


def shared_nested_bpc_shortfall_rows(selected_rows: list[dict[str, Any]]) -> dict[str, str]:
    """Selected rows that together spend more runs of one BPC than are
    owned, where the list cannot size the rest (no invention model, or a
    top-level user without an invention_bpc_plan), by row name with why
    (the UI warns)."""
    named: dict[int, str] = {}
    rows: list[tuple[dict[str, Any], int]] = []
    for row in _distinct_rows(selected_rows):
        mj = row.get("manufacturing_job") if isinstance(row, dict) else None
        if isinstance(mj, dict) and not _row_predates_later_batch_plans(mj):
            rows.append((mj, max(1, int(row.get("max_batches_total") or 1))))
            named[id(mj)] = str(row.get("type_name") or row.get("type_id") or "?")
    out: dict[str, str] = {}
    for bp, (excess, users) in sorted(_shared_bpc_plan(rows).unsized.items()):
        for mj in users:
            out[named[id(mj)]] = (
                f"shares owned BPC runs of blueprint {bp} with another selected item: "
                f"{excess} run{'s' if excess != 1 else ''} short, not in this list (build or buy that component separately)"
            )
    return out


def _minus(total: list[Any], part: list[Any]) -> list[dict[str, Any]]:
    """`total` lines less `part`'s quantity of the same type, per line."""
    part_by_type: dict[int, int] = {}
    for mat in part:
        if isinstance(mat, dict) and mat.get("type_id") is not None:
            part_by_type[int(mat["type_id"])] = part_by_type.get(int(mat["type_id"]), 0) + int(mat.get("quantity") or 0)
    out: list[dict[str, Any]] = []
    for mat in total:
        if not isinstance(mat, dict) or mat.get("type_id") is None:
            continue
        quantity = max(0, int(mat.get("quantity") or 0) - part_by_type.pop(int(mat["type_id"]), 0))
        if quantity > 0:
            out.append({**mat, "quantity": quantity, "take_quantity": 0, "buy_quantity": quantity})
    return out


def _blueprints_whose_bpc_runs_it_may_use(mj: dict[str, Any]) -> set[int] | None:
    """The blueprint types whose owned BPC runs a counted row may spend: its
    own and its nested blueprints. None when the row does not say (an
    overview from before I3 planned with BPCs): it may use any."""
    own = (mj.get("blueprint_sde") or {}).get("blueprint_type_id") if isinstance(mj.get("blueprint_sde"), dict) else None
    out = {int(own)} if own else set()
    left = mj.get(NESTED_RUNS_LEFT_KEY)
    if isinstance(left, dict):
        return out | {int(t) for t in left}
    if mj.get("blueprint_source_kind") in _NON_BPC_SOURCE_KINDS:
        return out
    return None


def _later_batch_nested_invention_lines(
    mj: dict[str, Any], later_nested: list[tuple[int, Any]], exclusive_blueprint: Any,
    *, skip: Any = frozenset(),
) -> list[dict[str, Any]]:
    """The nested inventions' inputs for batches 2..N, sized over all batches.

    Per nested blueprint: the runs batches 2..N invent (each later plan
    assumes no owned BPC run of it is left), less what is already there:
    the spare runs of the BPCs batch 1 invented (ceil(n / R) x R - n), and
    the owned BPC runs left after batch 1 when no other selected row may use
    them. BPCs = ceil(runs / R), each bought as whole attempts, as the row's
    own invention_bpc_plan does. So BPCs over all batches = ceil(B x n / R),
    not B x ceil(n / R); never less than the runs needed.
    """
    def plans(value: Any) -> list[dict[str, Any]]:
        return [p for p in (value or []) if isinstance(p, dict) and p.get("invented_blueprint_type_id") is not None]

    later_runs: dict[int, int] = {}
    model: dict[int, dict[str, Any]] = {}
    for count, value in later_nested:
        for entry in plans(value):
            bp = int(entry["invented_blueprint_type_id"])
            later_runs[bp] = later_runs.get(bp, 0) + int(count) * max(0, int(entry.get("invented_runs") or 0))
            model.setdefault(bp, entry)
            # The fewest runs per BPC and the most attempts per BPC: never short.
            model[bp] = {
                **model[bp],
                "runs_per_invented_blueprint": min(
                    max(1, int(model[bp].get("runs_per_invented_blueprint") or 1)),
                    max(1, int(entry.get("runs_per_invented_blueprint") or 1)),
                ),
                "whole_attempts_per_blueprint": max(
                    int(model[bp].get("whole_attempts_per_blueprint") or 0),
                    int(entry.get("whole_attempts_per_blueprint") or 0),
                ),
            }
    spare: dict[int, int] = {}
    for entry in plans(mj.get(NESTED_BPC_PLANS_KEY)):
        bp = int(entry["invented_blueprint_type_id"])
        n = max(0, int(entry.get("invented_runs") or 0))
        per_bpc = max(1, int(entry.get("runs_per_invented_blueprint") or 1))
        spare[bp] = spare.get(bp, 0) + (-(-n // per_bpc)) * per_bpc - n
    left = mj.get(NESTED_RUNS_LEFT_KEY)
    for type_key, runs in (left.items() if isinstance(left, dict) else []):
        try:
            bp, owned_left = int(type_key), max(0, int(runs or 0))
        except (TypeError, ValueError):
            continue
        if owned_left and exclusive_blueprint(bp):
            spare[bp] = spare.get(bp, 0) + owned_left

    out: list[dict[str, Any]] = []
    for bp, runs in later_runs.items():
        if bp in skip:
            continue
        entry = model[bp]
        to_invent = max(0, runs - spare.get(bp, 0))
        bpcs = -(-to_invent // int(entry["runs_per_invented_blueprint"]))
        attempts = bpcs * int(entry["whole_attempts_per_blueprint"])
        if attempts <= 0:
            continue
        per_attempt = entry.get(BPC_PLAN_LINES_KEY)
        for mat in (per_attempt.values() if isinstance(per_attempt, dict) else []):
            if not isinstance(mat, dict):
                continue
            quantity = int(mat.get("quantity") or 0) * attempts
            if quantity > 0:
                out.append({**mat, "quantity": quantity, "take_quantity": 0, "buy_quantity": quantity})
    return out


#: The source kinds of a row planned with build_from_bpc off: one job from an
#: owned or assumed BPO, every batch the same, so batch 1 x N is exact.
_NON_BPC_SOURCE_KINDS = frozenset({"owned_blueprint_original", "blueprint_sde_fallback"})


def _row_predates_later_batch_plans(mj: dict[str, Any]) -> bool:
    """A row not stamped with this LATER_BATCH_PLAN_VERSION that was (or
    may have been) planned with build_from_bpc: any source kind but the
    non-BPC ones, an unknown kind included."""
    if mj.get("later_batch_plan_version") == LATER_BATCH_PLAN_VERSION:
        return False
    return mj.get("blueprint_source_kind") not in _NON_BPC_SOURCE_KINDS


def _later_batch_plan_counts(schedule: Any, later_batches: int) -> dict[int, int]:
    """How many of batches 2..N use each plan of an ``extra_batch_schedule``.

    The schedule is {"plans": [...], "lead": [[plan index, count], ...],
    "cycle": [plan index, ...]}: batches 2, 3, ... take the lead entries in
    order (count batches each), then the cycle repeats for every batch after.
    Raises ValueError on a malformed schedule: scaling batch 1 instead is the
    under-buy the schedule exists to prevent.
    """
    plans = schedule.get("plans") if isinstance(schedule, dict) else None
    lead = schedule.get("lead") if isinstance(schedule, dict) else None
    cycle = schedule.get("cycle") if isinstance(schedule, dict) else None

    def valid_index(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and 0 <= value < len(plans or [])

    if (
        not isinstance(plans, list) or not plans or not all(isinstance(p, dict) for p in plans)
        or not isinstance(lead, list) or not isinstance(cycle, list) or not cycle
        or not all(valid_index(i) for i in cycle)
        or not all(
            isinstance(e, (list, tuple)) and len(e) == 2 and valid_index(e[0])
            and isinstance(e[1], int) and not isinstance(e[1], bool) and e[1] >= 0
            for e in lead
        )
    ):
        raise ValueError(f"malformed extra_batch_schedule: {schedule!r}")
    counts: dict[int, int] = {}
    remaining = max(0, int(later_batches))
    for plan_index, count in lead:
        used = min(int(count), remaining)
        if used > 0:
            counts[int(plan_index)] = counts.get(int(plan_index), 0) + used
        remaining -= used
    full_cycles, rest = divmod(remaining, len(cycle))
    for position, plan_index in enumerate(cycle):
        used = full_cycles + (1 if position < rest else 0)
        if used > 0:
            counts[int(plan_index)] = counts.get(int(plan_index), 0) + used
    return counts


def _later_batch_invention_lines(plan: dict[str, Any], batches: int) -> list[dict[str, Any]]:
    """The row's own invention inputs for batches 2..N, sized over all batches.

    BPCs needed for the whole order = ceil(max(0, batches x batch runs - owned
    BPC runs) / runs per invented BPC); batch 1 already invented
    ``batch_one_invented_blueprints`` of them (their leftover runs count). Each
    further BPC is ``whole_attempts_per_blueprint`` whole attempts (ceil(1 /
    chance)), so the datacore and decryptor lines scale together. Never less
    than whole attempts per BPC, so never an under-buy on the expected model.
    The lines take nothing from stock (stock is netted once over the list).
    """
    runs_per_bpc = max(1, int(plan.get("runs_per_invented_blueprint") or 1))
    runs_to_invent = max(
        0, batches * max(0, int(plan.get("batch_runs") or 0)) - max(0, int(plan.get("owned_blueprint_runs") or 0)),
    )
    bpcs_total = -(-runs_to_invent // runs_per_bpc)
    later_bpcs = max(0, bpcs_total - max(0, int(plan.get("batch_one_invented_blueprints") or 0)))
    attempts = later_bpcs * max(0, int(plan.get("whole_attempts_per_blueprint") or 0))
    if attempts <= 0:
        return []
    out: list[dict[str, Any]] = []
    per_attempt = plan.get(BPC_PLAN_LINES_KEY)
    for mat in (per_attempt.values() if isinstance(per_attempt, dict) else []):
        if not isinstance(mat, dict):
            continue
        quantity = int(mat.get("quantity") or 0) * attempts
        if quantity > 0:
            out.append({**mat, "quantity": quantity, "take_quantity": 0, "buy_quantity": quantity})
    return out


def _published_quantity(type_key: Any, quantity: Any) -> tuple[int, int] | None:
    """(type_id, quantity) of one published stock entry, None when malformed.

    A malformed entry counts as unpublished (the take bound applies): never
    more stock than the producer reported. A fraction truncates down.
    """
    try:
        type_id = int(type_key)
    except (TypeError, ValueError):
        return None
    if isinstance(quantity, bool) or not isinstance(quantity, (int, float)) or quantity < 0:
        return None
    return type_id, int(quantity)


def _batch_one_emptied_type_ids(mj: dict[str, Any]) -> set[int]:
    """The types a row's batch 1 bought some of (an explicit buy_quantity > 0).

    Batch 1's lists are the only ones planned against stock, and a line buys
    only what the stock it was planned against could not cover, so that row's
    take is the whole stock: the take bound is exact for these types.
    """
    out: set[int] = set()
    for key in (PROCUREMENT_KEY, INVENTION_KEY):
        value = mj.get(key)
        for mat in (value.values() if isinstance(value, dict) else []):
            if not isinstance(mat, dict) or "buy_quantity" not in mat:
                continue
            try:
                if int(mat["buy_quantity"]) > 0:
                    out.add(int(mat["type_id"]))
            except (KeyError, TypeError, ValueError):
                continue
    return out


def _buy_per_batch(mat: dict[str, Any], quantity_per_batch: int) -> int:
    """What one batch buys: the explicit buy_quantity, else everything.

    Without the split, nothing is known to be owned, whatever the strategy says:
    a "take" label on a merged take + buy line does not mean it is all owned.
    """
    if "buy_quantity" in mat:
        return int(mat["buy_quantity"])
    return quantity_per_batch


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None
