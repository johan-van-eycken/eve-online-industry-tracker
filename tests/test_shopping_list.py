from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from streamlit_ui.shopping_list import aggregate_shopping_list, aggregate_shopping_list_with_stale


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
        "manufacturing_job": {"procurement_materials": materials},
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
    mj: dict = {"procurement_materials": {}}
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
    except Exception:
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


# --- round 5 Task 5 fix round 1: overviews from before the later-batch plans --


def _round_four_row(batches: int, *, owned_top: bool = False, owned_nested: bool = False) -> dict:
    """A row as an overview cached before extra_batch_schedule existed: no
    later_batch_plan_version, batch 1 x N on the material lines."""
    row = _make_row(1001, "Item T2", batches, {"34": _owned_line(96, 0)})
    mj = row["manufacturing_job"]
    mj["job_segments"] = [{"runs": 10, "blueprint_material_efficiency": 4, "source": (
        "owned_blueprint_copy" if owned_top else "unowned_blueprint_copy")}]
    mj["recursive_activity_breakdown"] = {"manufacturing:5002": {
        "recommended_action": "build",
        "job_segments": [{"runs": 10, "source": "owned_blueprint_copy" if owned_nested else "invented_blueprint_copy"}],
        "nested": {},
    }}
    return row


@pytest.mark.parametrize(("owned_top", "owned_nested"), [(True, False), (False, True)])
def test_an_unstamped_row_on_an_owned_bpc_is_stale(owned_top, owned_nested) -> None:
    # Its later batches repeat batch 1, which may under-buy: ask for a refresh.
    _items, stale = aggregate_shopping_list_with_stale([_round_four_row(3, owned_top=owned_top, owned_nested=owned_nested)])
    assert stale == {1001: "Item T2"}


def test_an_unstamped_row_without_an_owned_bpc_or_one_batch_is_not_stale() -> None:
    assert aggregate_shopping_list_with_stale([_round_four_row(3)])[1] == {}
    assert aggregate_shopping_list_with_stale([_round_four_row(1, owned_top=True)])[1] == {}


def test_a_stamped_row_is_not_stale() -> None:
    row = _round_four_row(3, owned_top=True)
    row["manufacturing_job"]["later_batch_plan_version"] = 1
    assert aggregate_shopping_list_with_stale([row])[1] == {}


def test_the_tab_asks_to_refresh_an_unstamped_owned_bpc_row(monkeypatch) -> None:
    captions = _render_tab_captions(monkeypatch, [_round_four_row(3, owned_top=True)])
    assert any("older overview" in c for c in captions)
