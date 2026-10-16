from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from streamlit_ui.shopping_list import aggregate_shopping_list, aggregate_shopping_list_with_stale
from eve_online_industry_tracker.application.industry.shopping_list import (
    OLDER_OVERVIEW_REASON,
    rows_built_by_an_older_version,
    LATER_BATCH_PLAN_VERSION,
)


def _make_row(
    type_id: int,
    type_name: str,
    max_batches: int,
    materials: dict,
) -> dict:
    return {
        "type_id": type_id,
        "type_name": type_name,
        "max_batches_total": max_batches,
        # A current overview row (see the fix round 2 tests for older ones).
        "manufacturing_job": {"procurement_materials": materials, "later_batch_plan_version": LATER_BATCH_PLAN_VERSION},
    }


def test_aggregates_buy_quantities_across_items() -> None:
    """Two items both needing Tritanium (34) — quantities sum correctly."""
    row_a = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=2,
        materials={
            "34": {
                "type_id": 34,
                "type_name": "Tritanium",
                "quantity": 100,
                "buy_quantity": 100,
                "sourcing_strategy": "buy",
                "unit_price": 5.0,
            }
        },
    )
    row_b = _make_row(
        type_id=1002,
        type_name="Item B",
        max_batches=3,
        materials={
            "34": {
                "type_id": 34,
                "type_name": "Tritanium",
                "quantity": 50,
                "buy_quantity": 50,
                "sourcing_strategy": "buy",
                "unit_price": 5.0,
            }
        },
    )
    result = aggregate_shopping_list([row_a, row_b])
    assert len(result) == 1
    item = result[0]
    assert item["type_id"] == 34
    assert item["need"] == 2 * 100 + 3 * 50  # 350
    assert item["buy"] == 350
    assert item["unit_price"] == 5.0


def test_take_strategy_contributes_zero_to_buy() -> None:
    """A material fully in inventory (sourcing_strategy=take) should have buy=0."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "35": {
                "type_id": 35,
                "type_name": "Pyerite",
                "quantity": 200,
                "buy_quantity": 0,
                "sourcing_strategy": "take",
                "unit_price": 10.0,
            }
        },
    )
    result = aggregate_shopping_list([row])
    assert len(result) == 1
    assert result[0]["buy"] == 0
    assert result[0]["need"] == 200


def test_split_strategy_uses_buy_quantity() -> None:
    """A mixed-source material should only count buy_quantity toward buy."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "36": {
                "type_id": 36,
                "type_name": "Mexallon",
                "quantity": 100,
                "buy_quantity": 40,
                "take_quantity": 60,
                "sourcing_strategy": "split",
                "unit_price": 8.0,
            }
        },
    )
    result = aggregate_shopping_list([row])
    assert len(result) == 1
    assert result[0]["need"] == 100
    assert result[0]["buy"] == 40


def test_sorted_by_total_isk_descending() -> None:
    """Output is sorted by buy * unit_price descending."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "34": {
                "type_id": 34, "type_name": "Tritanium",
                "quantity": 10, "buy_quantity": 10,
                "sourcing_strategy": "buy", "unit_price": 5.0,
            },
            "35": {
                "type_id": 35, "type_name": "Pyerite",
                "quantity": 100, "buy_quantity": 100,
                "sourcing_strategy": "buy", "unit_price": 50.0,
            },
        },
    )
    result = aggregate_shopping_list([row])
    # Pyerite: 100 * 50 = 5000, Tritanium: 10 * 5 = 50 — Pyerite first
    assert result[0]["type_id"] == 35
    assert result[1]["type_id"] == 34


def test_empty_rows_returns_empty_list() -> None:
    assert aggregate_shopping_list([]) == []


def test_row_with_no_manufacturing_job_is_skipped() -> None:
    row = {"type_id": 1001, "type_name": "Broken Row", "max_batches_total": 1}
    result = aggregate_shopping_list([row])
    assert result == []


def test_fallback_when_buy_quantity_key_missing() -> None:
    """Without a take/buy split nothing is known to be owned: every strategy buys the full quantity."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "34": {
                "type_id": 34, "type_name": "Tritanium",
                "quantity": 100,
                # NO buy_quantity key
                "sourcing_strategy": "buy",
                "unit_price": 5.0,
            },
            "35": {
                "type_id": 35, "type_name": "Pyerite",
                "quantity": 50,
                # NO buy_quantity key
                "sourcing_strategy": "take",
                "unit_price": 10.0,
            },
        },
    )
    result = aggregate_shopping_list([row])
    trit = next(r for r in result if r["type_id"] == 34)
    pye = next(r for r in result if r["type_id"] == 35)
    assert trit["buy"] == 100
    assert pye["buy"] == 50


# --- owned stock is subtracted once, from the gross need of all batches -------


def _owned_line(quantity: int, take: int, *, type_id: int = 34, type_name: str = "Tritanium") -> dict:
    """One batch's take-or-buy line as the producer writes it."""
    return {
        "type_id": type_id,
        "type_name": type_name,
        "quantity": quantity,
        "take_quantity": take,
        "buy_quantity": quantity - take,
        "sourcing_strategy": "take" if take >= quantity else ("split" if take > 0 else "buy"),
        "unit_price": 5.0,
    }


def _line(result: list[dict], type_id: int = 34) -> tuple[int, int]:
    item = next(r for r in result if r["type_id"] == type_id)
    return item["need"], item["buy"]


def test_owned_stock_is_spent_once_across_batches() -> None:
    # 3 owned, 4 per batch, 5 batches: 4 x 5 - 3 = 17, not (4 - 3) x 5 = 5.
    row = _make_row(1001, "Item A", 5, {"34": _owned_line(4, 3)})
    assert _line(aggregate_shopping_list([row])) == (20, 17)


def test_single_batch_with_owned_stock_is_unchanged() -> None:
    row = _make_row(1001, "Item A", 1, {"34": _owned_line(4, 3)})
    assert _line(aggregate_shopping_list([row])) == (4, 1)


def test_no_stock_several_batches_is_unchanged() -> None:
    row = _make_row(1001, "Item A", 5, {"34": _owned_line(4, 0)})
    assert _line(aggregate_shopping_list([row])) == (20, 20)


def test_fully_owned_batch_still_buys_the_later_batches() -> None:
    # 4 owned covers batch 1 only: 4 x 5 - 4 = 16.
    row = _make_row(1001, "Item A", 5, {"34": _owned_line(4, 4)})
    assert _line(aggregate_shopping_list([row])) == (20, 16)


def test_stock_shared_by_two_products_is_subtracted_once() -> None:
    # Each row is planned against the full stock, so both report taking the
    # same 3 owned units. Need 4 x 2 + 4 x 3 = 20; the 3 count once: 17.
    row_a = _make_row(1001, "Item A", 2, {"34": _owned_line(4, 3)})
    row_b = _make_row(1002, "Item B", 3, {"34": _owned_line(4, 3)})
    assert _line(aggregate_shopping_list([row_a, row_b])) == (20, 17)


def test_shared_stock_never_assumes_more_than_any_row_reported() -> None:
    # Rows report 1 and 3 taken: owned is at least 3, never 1 + 3.
    row_a = _make_row(1001, "Item A", 1, {"34": _owned_line(4, 1)})
    row_b = _make_row(1002, "Item B", 1, {"34": _owned_line(4, 3)})
    assert _line(aggregate_shopping_list([row_a, row_b])) == (8, 5)


def test_invention_datacore_line_follows_the_same_rule() -> None:
    row = {
        "type_id": 5002,
        "type_name": "Item T2",
        "max_batches_total": 5,
        "manufacturing_job": {
            "procurement_materials": {},
            "invention_procurement_materials": {"204": _owned_line(4, 3, type_id=204, type_name="Datacore")},
            "later_batch_plan_version": LATER_BATCH_PLAN_VERSION,
        },
    }
    assert _line(aggregate_shopping_list([row]), 204) == (20, 17)


# --- fix round 1: a take line without a split comes from a stale overview ------


def _stale_take_line() -> dict:
    """A merged take + buy line from an overview cached before take_quantity existed."""
    return {"type_id": 34, "type_name": "Tritanium", "quantity": 10, "sourcing_strategy": "take", "unit_price": 5.0}


def test_stale_take_line_over_several_batches_buys_the_full_need() -> None:
    row = _make_row(1001, "Item A", 5, {"34": _stale_take_line()})
    assert _line(aggregate_shopping_list([row])) == (50, 50)


def test_stale_take_line_for_one_batch_buys_the_full_need() -> None:
    row = _make_row(1001, "Item A", 1, {"34": _stale_take_line()})
    assert _line(aggregate_shopping_list([row])) == (10, 10)


def test_stale_lines_log_one_debug_line_naming_the_types(caplog) -> None:
    # DEBUG, not WARNING: it runs on every UI rerun, and the UI caption is the
    # real signal (Task I fix round 1).
    row_a = _make_row(1001, "Item A", 5, {"34": _stale_take_line()})
    row_b = _make_row(1002, "Item B", 2, {"34": _stale_take_line(), "35": {**_stale_take_line(), "type_id": 35, "type_name": "Pyerite"}})
    with caplog.at_level("DEBUG"):
        aggregate_shopping_list([row_a, row_b])
    assert not [r for r in caplog.records if r.levelname == "WARNING"]
    debug = [r.getMessage() for r in caplog.records if r.levelname == "DEBUG" and "older overview" in r.getMessage()]
    assert len(debug) == 1
    assert "Tritanium" in debug[0] and "Pyerite" in debug[0]
    assert "refresh" in debug[0].lower()


def test_lines_with_a_split_log_no_warning(caplog) -> None:
    row = _make_row(1001, "Item A", 5, {"34": _owned_line(4, 3)})
    with caplog.at_level("WARNING"):
        aggregate_shopping_list([row])
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


# --- round 3 final wave I1: datacores for batches past the owned T2 BPCs -------


def _invention_row(batches: int, *, batch_one: dict | None, extra: dict | None) -> dict:
    mj: dict = {"procurement_materials": {}, "later_batch_plan_version": LATER_BATCH_PLAN_VERSION}
    if batch_one is not None:
        mj["invention_procurement_materials"] = {"204": batch_one}
    if extra is not None:
        mj["invention_procurement_materials_per_extra_batch"] = {"204": extra}
    return {"type_id": 5002, "type_name": "Item T2", "max_batches_total": batches, "manufacturing_job": mj}


def _datacore(quantity: int, take: int = 0) -> dict:
    return _owned_line(quantity, take, type_id=204, type_name="Datacore")


def test_batch_one_covered_by_an_owned_bpc_buys_the_later_batches() -> None:
    row = _invention_row(5, batch_one=None, extra=_datacore(4))
    assert _line(aggregate_shopping_list([row]), 204) == (16, 16)


def test_partial_bpc_cover_adds_full_batches_after_batch_one() -> None:
    row = _invention_row(3, batch_one=_datacore(12), extra=_datacore(20))
    assert _line(aggregate_shopping_list([row]), 204) == (52, 52)


def test_extra_batch_list_nets_owned_stock_once() -> None:
    row = _invention_row(5, batch_one=_datacore(4, take=3), extra=_datacore(4))
    assert _line(aggregate_shopping_list([row]), 204) == (20, 17)


def test_overview_without_an_extra_batch_list_scales_batch_one() -> None:
    # Cached before the producer wrote the extra-batch list: batch 1 x batches.
    row = _invention_row(5, batch_one=_datacore(4, take=3), extra=None)
    assert _line(aggregate_shopping_list([row]), 204) == (20, 17)


# --- round 3 final wave M5/M6 ---------------------------------------------------


def test_unit_price_is_the_first_known_price_not_the_first_line_seen() -> None:
    unpriced = {**_owned_line(4, 0), "unit_price": None}
    priced = {**_owned_line(4, 0), "unit_price": 7.5}
    rows = [_make_row(1001, "Item A", 1, {"34": unpriced}), _make_row(1002, "Item B", 1, {"34": priced})]
    assert aggregate_shopping_list(rows)[0]["unit_price"] == 7.5


def test_unit_price_stays_none_when_no_line_has_one() -> None:
    unpriced = {**_owned_line(4, 0), "unit_price": None}
    assert aggregate_shopping_list([_make_row(1001, "Item A", 1, {"34": unpriced})])[0]["unit_price"] is None


def test_with_stale_returns_the_stale_type_ids_and_the_same_items() -> None:
    from streamlit_ui.shopping_list import aggregate_shopping_list_with_stale

    rows = [_make_row(1001, "Item A", 2, {"34": _stale_take_line()})]
    items, stale = aggregate_shopping_list_with_stale(rows)
    assert items == aggregate_shopping_list(rows)
    assert stale == {34: "Tritanium"}


def test_with_stale_is_empty_for_lines_with_a_split() -> None:
    from streamlit_ui.shopping_list import aggregate_shopping_list_with_stale

    _, stale = aggregate_shopping_list_with_stale([_make_row(1001, "Item A", 2, {"34": _owned_line(4, 3)})])
    assert stale == {}


def _render_tab_captions(monkeypatch, rows: list[dict]) -> list[str]:
    from streamlit_ui.pages import portfolio_planner as page

    captions: list[str] = []
    monkeypatch.setattr(page.st, "caption", lambda text, *a, **k: captions.append(str(text)))
    monkeypatch.setattr(page.st, "markdown", lambda *a, **k: None)
    monkeypatch.setattr(page.st, "info", lambda *a, **k: None)
    monkeypatch.setattr(page.st, "multiselect", lambda label, options, **k: list(options))
    monkeypatch.setattr(page.st, "stop", lambda: None)

    class _Stop(Exception):
        pass

    # Everything after the aggregation is table rendering; stop there.
    monkeypatch.setattr(page.st, "metric", lambda *a, **k: (_ for _ in ()).throw(_Stop()), raising=False)
    monkeypatch.setattr(page.st, "columns", lambda *a, **k: (_ for _ in ()).throw(_Stop()), raising=False)
    try:
        page._render_shopping_list_tab(rows)
    except _Stop:
        pass
    return captions


def test_tab_shows_the_stale_caption_and_the_new_stock_rule(monkeypatch) -> None:
    captions = _render_tab_captions(monkeypatch, [_make_row(1001, "Item A", 2, {"34": _stale_take_line()})])
    assert any("older overview" in c and "refresh for exact stock use" in c for c in captions)
    assert any("subtracted once" in c for c in captions)
    assert not any("sourcing strategy" in c for c in captions)


def test_tab_shows_no_stale_caption_for_current_lines(monkeypatch) -> None:
    captions = _render_tab_captions(monkeypatch, [_make_row(1001, "Item A", 2, {"34": _owned_line(4, 3)})])
    assert not any("older overview" in c for c in captions)


# --- round 4 Task I R3: unknown datacore need notice ----------------------------


def _unknown_need_row(batches: int, from_batch: int) -> dict:
    row = _invention_row(batches, batch_one=None, extra=None)
    row["manufacturing_job"]["invention_datacore_need_unknown"] = {
        "reason": "batches past the owned T2 BPC runs: no invention success probability", "from_batch": from_batch,
    }
    return row


def test_unknown_need_rows_names_rows_whose_batches_reach_the_unknown_need() -> None:
    from streamlit_ui.shopping_list import invention_need_unknown_rows

    assert list(invention_need_unknown_rows([_unknown_need_row(3, 2)])) == ["Item T2"]
    assert invention_need_unknown_rows([_unknown_need_row(1, 2)]) == {}
    assert list(invention_need_unknown_rows([_unknown_need_row(1, 1)])) == ["Item T2"]
    assert invention_need_unknown_rows([_make_row(1001, "Item A", 2, {"34": _owned_line(4, 3)})]) == {}


def test_tab_warns_about_an_unknown_datacore_need(monkeypatch) -> None:
    from streamlit_ui.pages import portfolio_planner as page

    warnings: list[str] = []
    monkeypatch.setattr(page.st, "warning", lambda text, *a, **k: warnings.append(str(text)))
    _render_tab_captions(monkeypatch, [_unknown_need_row(3, 2)])
    assert len(warnings) == 1
    assert "Item T2" in warnings[0] and "datacore" in warnings[0].lower()


def test_tab_shows_no_unknown_need_warning_for_known_rows(monkeypatch) -> None:
    from streamlit_ui.pages import portfolio_planner as page

    warnings: list[str] = []
    monkeypatch.setattr(page.st, "warning", lambda text, *a, **k: warnings.append(str(text)))
    _render_tab_captions(monkeypatch, [_make_row(1001, "Item A", 2, {"34": _owned_line(4, 3)})])
    assert warnings == []


def test_unknown_need_rows_log_debug_not_warning(caplog) -> None:
    from streamlit_ui.shopping_list import invention_need_unknown_rows

    with caplog.at_level("DEBUG"):
        assert list(invention_need_unknown_rows([_unknown_need_row(3, 2)])) == ["Item T2"]
    assert not [r for r in caplog.records if r.levelname == "WARNING"]
    assert any("datacore need unknown" in r.getMessage() for r in caplog.records if r.levelname == "DEBUG")


# --- round 5 Task 5: batches 2..N from their own plans ---------------------------
# A row whose later batches differ from batch 1 carries
# manufacturing_job.extra_batch_schedule: distinct later-batch plans, and which
# plan each of batches 2..N uses ("lead" run-length [plan, count] pairs, then
# "cycle" repeating). Every plan line is one batch's pure requirement (take 0).


def _plan(tritanium: int, *, datacores: int = 0, nested: dict | None = None, jobs=((10, 2, 4),)) -> dict:
    return {
        "job_segments": [
            {"runs": r, "blueprint_material_efficiency": me, "blueprint_time_efficiency": te} for r, me, te in jobs
        ],
        "procurement_materials": {"34": _owned_line(tritanium, 0)},
        "invention_procurement_materials": (
            {"204": _owned_line(datacores, 0, type_id=204, type_name="Datacore")} if datacores else {}
        ),
        "invention_procurement_materials_nested": dict(nested or {}),
    }


def _scheduled_row(batches: int, *, batch_one: int, plans: list[dict], lead: list, cycle: list[int]) -> dict:
    row = _make_row(1001, "Item T2", batches, {"34": _owned_line(batch_one, 0)})
    row["manufacturing_job"]["extra_batch_schedule"] = {"plans": plans, "lead": lead, "cycle": cycle}
    return row


def test_one_later_batch_plan_replaces_batch_one_for_batches_two_to_n() -> None:
    # An owned ME 4 BPC covers batch 1 (96); batches 2..N invent at ME 2 (98).
    row = _scheduled_row(3, batch_one=96, plans=[_plan(98)], lead=[], cycle=[0])
    assert _line(aggregate_shopping_list([row])) == (96 + 2 * 98, 96 + 2 * 98)


def test_the_later_batch_plans_are_ignored_for_a_single_batch() -> None:
    row = _scheduled_row(1, batch_one=96, plans=[_plan(98)], lead=[], cycle=[0])
    assert _line(aggregate_shopping_list([row])) == (96, 96)


def test_later_batch_plans_net_owned_stock_once() -> None:
    row = _scheduled_row(3, batch_one=96, plans=[_plan(98)], lead=[], cycle=[0])
    row["manufacturing_job"]["procurement_materials"]["34"] = _owned_line(96, 10)
    assert _line(aggregate_shopping_list([row])) == (292, 282)


def _split_bpc_row(batches: int) -> dict:
    # 10-run batches on 11-run invented BPCs: batch 1 is one job (96); batch 2
    # is 1 run on BPC 1's leftover + 9 on BPC 2 (97, two ceilings); batch 3 is
    # 2 + 8 (97). The lead names batch 2, the cycle batches 3, 4, 5, ...
    plans = [_plan(96), _plan(97, jobs=((1, 4, 14), (9, 4, 14))), _plan(97, jobs=((2, 4, 14), (8, 4, 14)))]
    return _scheduled_row(batches, batch_one=96, plans=plans, lead=[[1, 1]], cycle=[2, 0])


@pytest.mark.parametrize(
    ("batches", "need"),
    [(1, 96), (2, 96 + 97), (3, 96 + 97 + 97), (4, 96 + 97 + 97 + 96), (7, 96 + 97 + 2 * (97 + 96) + 97)],
)
def test_each_later_batch_is_sized_from_its_own_plan(batches, need) -> None:
    assert _line(aggregate_shopping_list([_split_bpc_row(batches)])) == (need, need)


def test_a_lead_run_length_covers_several_batches_before_the_cycle() -> None:
    # A 1-run batch on a 10-run owned BPC: batches 2..10 on the owned BPC
    # (plan 0), from batch 11 on invented (plan 1).
    row = _scheduled_row(14, batch_one=10, plans=[_plan(10), _plan(11)], lead=[[0, 9]], cycle=[1])
    assert _line(aggregate_shopping_list([row])) == (10 + 9 * 10 + 4 * 11,) * 2
    row["max_batches_total"] = 5  # the lead is cut at batch 5
    assert _line(aggregate_shopping_list([row])) == (50, 50)


def test_without_a_bpc_plan_each_later_batch_buys_its_plans_invention_inputs() -> None:
    plans = [_plan(96, datacores=4), _plan(97, datacores=6)]
    row = _scheduled_row(4, batch_one=96, plans=plans, lead=[[1, 2]], cycle=[0])  # batches 2..4: 1, 1, 0
    assert _line(aggregate_shopping_list([row]), 204) == (6 + 6 + 4, 6 + 6 + 4)


def test_with_a_bpc_plan_each_later_batch_adds_only_its_nested_inputs() -> None:
    nested = {"205": _owned_line(1, 0, type_id=205, type_name="Nested")}
    plans = [_plan(96, datacores=4), _plan(97, datacores=6, nested=nested)]
    row = _scheduled_row(3, batch_one=96, plans=plans, lead=[[1, 1]], cycle=[1, 0])  # batches 2, 3: 1, 1
    row["manufacturing_job"]["invention_bpc_plan"] = {
        "batch_runs": 10, "runs_per_invented_blueprint": 11, "whole_attempts_per_blueprint": 2,
        "owned_blueprint_runs": 0, "batch_one_invented_blueprints": 1,
        "per_attempt_materials": {"204": {"type_id": 204, "type_name": "Datacore", "quantity": 2}},
    }
    result = aggregate_shopping_list([row])
    # 30 runs = 3 BPCs, batch 1 invented 1: 2 more x 2 attempts x 2 = 8.
    assert _line(result, 204) == (8, 8)
    assert _line(result, 205) == (2, 2)


@pytest.mark.parametrize(
    "schedule",
    [
        {"plans": [], "lead": [], "cycle": [0]},
        {"plans": [_plan(98)], "lead": [], "cycle": []},
        {"plans": [_plan(98)], "lead": [[3, 1]], "cycle": [0]},
        {"plans": [_plan(98)], "lead": [[0, -1]], "cycle": [0]},
    ],
)
def test_a_malformed_schedule_fails_loud(schedule) -> None:
    # Never fall back to batch 1 x N: that is the under-buy this list prevents.
    row = _make_row(1001, "Item T2", 3, {"34": _owned_line(96, 0)})
    row["manufacturing_job"]["extra_batch_schedule"] = schedule
    with pytest.raises(ValueError, match="extra_batch_schedule"):
        aggregate_shopping_list([row])


# --- round 5 Task 5 fix rounds 1-2: overviews from before the later-batch plans --
# A build_from_bpc row without later_batch_plan_version comes from an older
# overview whose later batches repeat batch 1, which can under-buy. Its
# quantities are never shown: the list names it and asks for a refresh.


def _round_four_row(batches: int, *, kind: str = "owned_blueprint_copy", type_id: int = 1001, name: str = "Item T2") -> dict:
    """A row as an overview cached before extra_batch_schedule existed."""
    row = _make_row(type_id, name, batches, {"34": _owned_line(96, 0)})
    mj = row["manufacturing_job"]
    del mj["later_batch_plan_version"]
    mj["blueprint_source_kind"] = kind
    mj["job_segments"] = [{"runs": 10, "blueprint_material_efficiency": 4, "source": kind}]
    return row


@pytest.mark.parametrize("kind", ["owned_blueprint_copy", "unowned_blueprint_copy", "copied_from_owned_blueprint_original", None])
@pytest.mark.parametrize("batches", [1, 3])
def test_an_unstamped_build_from_bpc_row_is_refused(kind, batches) -> None:
    # Owned, invented (the fully-invented 96-vs-97 case) or copied, any batch
    # count; an unknown source kind is refused too.
    row = _round_four_row(batches, kind=kind or "x")
    if kind is None:
        del row["manufacturing_job"]["blueprint_source_kind"]
    assert rows_built_by_an_older_version([row]) == {"Item T2": OLDER_OVERVIEW_REASON}
    assert aggregate_shopping_list([row]) == []


def test_only_the_unstamped_rows_are_left_out() -> None:
    current = _make_row(1002, "Item Current", 2, {"34": _owned_line(10, 0)})
    old = _round_four_row(3, type_id=1001, name="Item Old")
    assert _line(aggregate_shopping_list([current, old])) == (20, 20)
    assert list(rows_built_by_an_older_version([current, old])) == ["Item Old"]


@pytest.mark.parametrize("kind", ["owned_blueprint_original", "blueprint_sde_fallback"])
def test_an_unstamped_row_planned_without_bpcs_is_not_refused(kind) -> None:
    # build_from_bpc off: one job from a BPO (owned or assumed), every batch
    # the same, so an older overview's batch 1 x N is exact.
    row = _round_four_row(3, kind=kind)
    assert rows_built_by_an_older_version([row]) == {}
    assert _line(aggregate_shopping_list([row])) == (288, 288)


def test_the_tab_warns_and_shows_no_numbers_for_an_unstamped_row(monkeypatch) -> None:
    from streamlit_ui.pages import portfolio_planner as page

    warnings: list[str] = []
    monkeypatch.setattr(page.st, "warning", lambda text, *a, **k: warnings.append(str(text)))
    infos: list[str] = []
    monkeypatch.setattr(page.st, "info", lambda text, *a, **k: infos.append(str(text)))
    _render_tab_captions(monkeypatch, [_round_four_row(3)])
    assert any("Item T2" in w and OLDER_OVERVIEW_REASON in w for w in warnings)


def _render_tab_output(monkeypatch, rows: list[dict]) -> dict[str, list]:
    """Everything the tab writes, rendered to the end (no early stop)."""
    from streamlit_ui.pages import portfolio_planner as page

    out: dict[str, list] = {k: [] for k in ("info", "warning", "caption", "markdown", "dataframe", "code")}
    for name in out:
        monkeypatch.setattr(page.st, name, lambda obj, *a, _n=name, **k: out[_n].append(obj))
    monkeypatch.setattr(page.st, "multiselect", lambda label, options, **k: list(options))
    page._render_shopping_list_tab(rows)
    return out


def test_the_tab_shows_no_numbers_of_a_refused_row_beside_a_current_one(monkeypatch) -> None:
    current = _make_row(1002, "Item Current", 2, {"35": _owned_line(10, 0, type_id=35, type_name="Pyerite")})
    out = _render_tab_output(monkeypatch, [current, _round_four_row(3, name="Item Old")])
    (df,) = out["dataframe"]
    # Only the current row's Pyerite line (2 x 10); the refused row's 3 x 96 Tritanium is absent.
    assert df.to_dict("records") == [
        {"Item": "Pyerite", "Need": "20", "Owned (est.)": "0", "Buy": "20",
         "Unit Price": "5", "Total ISK": "100"},
    ]
    assert not any("Tritanium" in str(c) for c in out["code"])


def test_the_tab_asks_for_a_refresh_when_every_selected_row_is_refused(monkeypatch) -> None:
    out = _render_tab_output(monkeypatch, [_round_four_row(3)])
    assert out["dataframe"] == [] and out["code"] == []
    assert not any("No buy-sourced materials" in str(i) for i in out["info"])
    assert any("refresh" in str(i).lower() for i in out["info"])


# --- round 5 Task 7: net against the published hangar stock (C8, C9) -----------
# Every row of one overview is planned against the same hangar stock and
# publishes it (manufacturing_job.owned_stock_by_type_id) for the types on its
# buy lists, so the aggregated need is netted against the real stock once.


def _with_stock(row: dict, stock: dict) -> dict:
    row["manufacturing_job"]["owned_stock_by_type_id"] = {str(k): v for k, v in stock.items()}
    return row


def test_published_stock_is_netted_exactly_once() -> None:
    # 10 owned; two rows each take 8 of them. The take bound said 8 owned
    # (buy 8); the stock the producer planned against says 10 (buy 6).
    row_a = _with_stock(_make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)}), {34: 10})
    row_b = _with_stock(_make_row(1002, "Item B", 1, {"34": _owned_line(8, 8)}), {34: 10})
    assert _line(aggregate_shopping_list([row_a, row_b])) == (16, 6)


def test_extra_batch_datacores_net_the_published_hangar_stock() -> None:
    # C8: an owned BPC covers batch 1, so no row took the 3 owned datacores.
    row = _with_stock(_invention_row(5, batch_one=None, extra=_datacore(4)), {204: 3})
    assert _line(aggregate_shopping_list([row]), 204) == (16, 13)


def test_a_row_without_published_stock_keeps_the_take_bound() -> None:
    row_a = _make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)})
    row_b = _with_stock(_make_row(1002, "Item B", 1, {"34": _owned_line(8, 3)}), {34: 5})
    # Published 5 < the other row's take 8: owned is 8, never less than a take.
    assert _line(aggregate_shopping_list([row_a, row_b])) == (16, 8)


def test_published_stock_nets_later_batch_plans_and_batch_one_takes_once() -> None:
    # 50 owned; batch 1 needs 20 and takes them all; batches 2 and 3 (their
    # own plan, take 0) need 98 each. Need 20 + 2 x 98 = 216; the 50 count
    # once: 166 (the take bound said 20 owned, buy 196).
    row = _scheduled_row(3, batch_one=20, plans=[_plan(98)], lead=[], cycle=[0])
    row["manufacturing_job"]["procurement_materials"]["34"] = _owned_line(20, 20)
    assert _line(aggregate_shopping_list([_with_stock(row, {34: 50})])) == (216, 166)


def test_published_stock_reaches_the_bpc_plan_datacores() -> None:
    # The invention_bpc_plan's later-batch datacores take nothing from stock;
    # 8 needed (2 BPCs x 2 attempts x 2), 3 owned: buy 5.
    row = _make_row(1001, "Item T2", 3, {})
    row["manufacturing_job"]["invention_bpc_plan"] = {
        "batch_runs": 10, "runs_per_invented_blueprint": 10, "whole_attempts_per_blueprint": 2,
        "owned_blueprint_runs": 10, "batch_one_invented_blueprints": 0,
        "per_attempt_materials": {"204": {"type_id": 204, "type_name": "Datacore", "quantity": 2}},
    }
    assert _line(aggregate_shopping_list([_with_stock(row, {204: 3})]), 204) == (8, 5)


def test_published_stock_never_exceeds_the_need() -> None:
    row = _with_stock(_make_row(1001, "Item A", 2, {"34": _owned_line(4, 4)}), {34: 100})
    assert _line(aggregate_shopping_list([row])) == (8, 0)


@pytest.mark.parametrize("bad", [None, "x", -5, 2.5])
def test_a_malformed_published_quantity_counts_as_the_take_bound(bad) -> None:
    # Never more than a take a row reported; 2.5 truncates to 2 < take 3.
    row = _with_stock(_make_row(1001, "Item A", 2, {"34": _owned_line(4, 3)}), {34: bad})
    assert _line(aggregate_shopping_list([row])) == (8, 5)


def test_a_malformed_published_quantity_where_the_take_may_understate_is_stale() -> None:
    row = _with_stock(_make_row(1001, "Item A", 2, {"34": _owned_line(4, 4)}), {34: "x"})
    assert aggregate_shopping_list_with_stale([row])[1] == {34: "Tritanium"}


def test_a_type_missing_from_a_rows_published_stock_keeps_the_take_bound() -> None:
    row = _with_stock(_make_row(1001, "Item A", 2, {"34": _owned_line(4, 3)}), {35: 9})
    assert _line(aggregate_shopping_list([row])) == (8, 5)


def test_published_stock_marks_nothing_stale() -> None:
    row = _with_stock(_make_row(1001, "Item A", 3, {"34": _owned_line(4, 4)}), {34: 6})
    _, stale = aggregate_shopping_list_with_stale([row])
    assert stale == {}


def test_a_row_without_published_stock_is_stale_where_the_take_may_understate_it() -> None:
    # Took all 4 it needed: the hangar may hold more (C9). Datacores only on a
    # later-batch list: none taken, the hangar may hold some (C8). Both may
    # over-buy, so the caption asks for a refresh.
    row = _make_row(1001, "Item A", 3, {"34": _owned_line(4, 4)})
    row["manufacturing_job"]["invention_procurement_materials_per_extra_batch"] = {"204": _datacore(4)}
    _, stale = aggregate_shopping_list_with_stale([row])
    assert stale == {34: "Tritanium", 204: "Datacore"}


def test_a_row_without_published_stock_is_exact_where_batch_one_bought() -> None:
    # Batch 1 bought some: its take emptied the stock it was planned against,
    # so the take is the whole stock and nothing is stale.
    rows = [
        _make_row(1001, "Item A", 3, {"34": _owned_line(4, 3)}),
        _make_row(1002, "Item B", 3, {"35": _owned_line(4, 0, type_id=35, type_name="Pyerite")}),
    ]
    _, stale = aggregate_shopping_list_with_stale(rows)
    assert stale == {}


def test_published_stock_on_another_row_covers_a_keyless_row() -> None:
    # The same overview, one pool: the published figure is exact for both.
    old = _make_row(1001, "Item A", 2, {"34": _owned_line(4, 4)})
    new = _with_stock(_make_row(1002, "Item B", 2, {"34": _owned_line(4, 4)}), {34: 7})
    items, stale = aggregate_shopping_list_with_stale([old, new])
    assert _line(items) == (16, 9)
    assert stale == {}


# --- round 5 Task 7 fix round 1 ---------------------------------------------------


def _pooled(row: dict, stock: dict, pool: str) -> dict:
    _with_stock(row, stock)["manufacturing_job"]["owned_stock_pool_id"] = pool
    return row


def test_rows_from_one_stock_pool_keep_the_published_figure() -> None:
    from streamlit_ui.shopping_list import rows_from_different_stock_pools

    rows = [
        _pooled(_make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)}), {34: 10}, "p1"),
        _pooled(_make_row(1002, "Item B", 1, {"34": _owned_line(8, 8)}), {34: 10}, "p1"),
    ]
    assert _line(aggregate_shopping_list(rows)) == (16, 6)
    assert rows_from_different_stock_pools(rows) is False


def test_rows_from_different_stock_pools_net_the_smallest_figure_and_warn() -> None:
    from streamlit_ui.shopping_list import rows_from_different_stock_pools

    # Refresh 1 saw 12, refresh 2 saw 9: net 9 (never more than either saw).
    rows = [
        _pooled(_make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)}), {34: 12}, "p1"),
        _pooled(_make_row(1002, "Item B", 1, {"34": _owned_line(8, 8)}), {34: 9}, "p2"),
    ]
    assert _line(aggregate_shopping_list(rows)) == (16, 7)
    assert rows_from_different_stock_pools(rows) is True


def test_mixed_stock_pools_never_net_less_than_a_take() -> None:
    rows = [
        _pooled(_make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)}), {34: 12}, "p1"),
        _pooled(_make_row(1002, "Item B", 1, {"34": _owned_line(8, 3)}), {34: 3}, "p2"),
    ]
    assert _line(aggregate_shopping_list(rows)) == (16, 8)


def test_the_tab_warns_about_rows_from_different_refreshes(monkeypatch) -> None:
    from streamlit_ui.pages import portfolio_planner as page

    warnings: list[str] = []
    monkeypatch.setattr(page.st, "warning", lambda text, *a, **k: warnings.append(str(text)))
    rows = [
        _pooled(_make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)}), {34: 12}, "p1"),
        _pooled(_make_row(1002, "Item B", 1, {"34": _owned_line(8, 8)}), {34: 9}, "p2"),
    ]
    _render_tab_captions(monkeypatch, rows)
    assert any("different refreshes" in w and "refresh before buying" in w for w in warnings)


def test_the_tab_does_not_warn_for_one_refresh(monkeypatch) -> None:
    from streamlit_ui.pages import portfolio_planner as page

    warnings: list[str] = []
    monkeypatch.setattr(page.st, "warning", lambda text, *a, **k: warnings.append(str(text)))
    rows = [
        _pooled(_make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)}), {34: 12}, "p1"),
        _pooled(_make_row(1002, "Item B", 1, {"34": _owned_line(8, 8)}), {34: 12}, "p1"),
    ]
    _render_tab_captions(monkeypatch, rows)
    assert not any("different refreshes" in w for w in warnings)


def test_a_type_emptied_by_another_keyless_row_is_not_stale() -> None:
    # Row B's batch 1 bought Tritanium: its take is the whole stock, so the
    # take bound is exact for row A's line of that type too.
    rows = [
        _make_row(1001, "Item A", 3, {"34": _owned_line(4, 4)}),
        _make_row(1002, "Item B", 3, {"34": _owned_line(9, 6)}),
    ]
    items, stale = aggregate_shopping_list_with_stale(rows)
    assert _line(items) == (39, 33)
    assert stale == {}


def test_the_buy_list_keys_cover_every_line_the_list_reads() -> None:
    # The producer publishes stock for the types under these keys; the list
    # reads its lines from the same keys, so the two cannot drift.
    from eve_online_industry_tracker.application.industry import shopping_list as sl
    from eve_online_industry_tracker.application.industry.service import IndustryService

    def line(type_id: int) -> dict:
        return _owned_line(2, 0, type_id=type_id, type_name=f"T{type_id}")

    plan = {key: {str(300 + i): line(300 + i)} for i, key in enumerate(sl.LATER_BATCH_PLAN_BUY_LIST_KEYS)}
    plan["job_segments"] = []
    base = {key: {str(200 + i): line(200 + i)} for i, key in enumerate(sl.ROW_BUY_LIST_KEYS)}
    for mj in (
        {**base, "later_batch_plan_version": LATER_BATCH_PLAN_VERSION},
        {
            **base, "later_batch_plan_version": LATER_BATCH_PLAN_VERSION,
            sl.BPC_PLAN_KEY: {
                "batch_runs": 10, "runs_per_invented_blueprint": 10, "whole_attempts_per_blueprint": 1,
                "owned_blueprint_runs": 0, "batch_one_invented_blueprints": 1,
                sl.BPC_PLAN_LINES_KEY: {"400": {"type_id": 400, "type_name": "T400", "quantity": 1}},
            },
            sl.EXTRA_BATCH_SCHEDULE_KEY: {"plans": [plan], "lead": [], "cycle": [0]},
        },
    ):
        read = {int(mat["type_id"]) for mat, _n in sl._row_lines_with_batch_counts(mj, 5)}
        assert read and read <= set(IndustryService._buy_line_type_ids(mj))


# --- Task 10 fix round 2, item 4: shared nested BPC runs without an invention model --

def _sharing_row(type_id, name, used, owned):
    row = _make_row(type_id, name, 1, {})
    row["manufacturing_job"].update({
        "blueprint_source_kind": "owned_blueprint_original",
        "nested_invention_bpc_plans": [], "nested_blueprint_runs_left": {"8888": owned - used},
        "nested_blueprint_runs_used": {"8888": used}, "nested_blueprint_runs_owned": {"8888": owned},
    })
    return row


def test_a_shared_nested_bpc_shortfall_without_an_invention_model_is_named() -> None:
    from eve_online_industry_tracker.application.industry.shopping_list import shared_nested_bpc_shortfall_rows

    rows = [_sharing_row(1, "Item A", 2, 3), _sharing_row(2, "Item B", 2, 3)]
    named = shared_nested_bpc_shortfall_rows(rows)
    assert set(named) == {"Item A", "Item B"}
    assert "1 run" in named["Item A"]
    assert shared_nested_bpc_shortfall_rows(rows[:1]) == {}
    assert shared_nested_bpc_shortfall_rows([_sharing_row(1, "A", 1, 3), _sharing_row(2, "B", 2, 3)]) == {}


def test_the_tab_warns_about_a_shared_nested_bpc_shortfall_it_cannot_size(monkeypatch) -> None:
    out = _render_tab_output(monkeypatch, [_sharing_row(1, "Item A", 2, 3), _sharing_row(2, "Item B", 2, 3)])
    assert any("Item A" in str(w) and "Item B" in str(w) and "short" in str(w) for w in out["warning"])


def test_a_row_from_before_this_plan_version_is_refused() -> None:
    # Fix round 3: the shared-BPC netting needs keys a version-1 row lacks.
    row = _make_row(1001, "Item Old", 2, {"34": _owned_line(10, 0)})
    row["manufacturing_job"]["later_batch_plan_version"] = LATER_BATCH_PLAN_VERSION - 1
    row["manufacturing_job"]["blueprint_source_kind"] = "owned_blueprint_copy"
    assert rows_built_by_an_older_version([row]) == {"Item Old": OLDER_OVERVIEW_REASON}
    assert aggregate_shopping_list([row]) == []
    assert LATER_BATCH_PLAN_VERSION >= 2
