# tests/test_shopping_list_builder.py
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import (
    ShoppingListBuilder,
)


class _NoBlueprints:
    def is_blueprint(self, type_id):
        return False

    def prefetch(self, type_ids):
        return None


class _BlueprintsAre:
    def __init__(self, *ids):
        self._ids = set(ids)

    def is_blueprint(self, type_id):
        return int(type_id) in self._ids

    def prefetch(self, type_ids):
        return None


class _Admin:
    def get(self, section, key, default=None):
        return default


def _action(runs, type_id=12345, blueprint_type_id=999):
    return AssignedAction(
        type_id=type_id, type_name="Widget", action_type="manufacture",
        character_id=1, character_name="Pilot", quantity=None, runs=runs,
        estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
    )


def _sub_manufacture_action(quantity, type_id=12345):
    # character_assigner.py:229-237 builds sub_manufacture actions with the
    # target component count in `quantity` and `runs` left None -- the
    # builder must derive an effective run count from `quantity`, not read
    # `action.runs` directly.
    return AssignedAction(
        type_id=type_id, type_name="Widget", action_type="sub_manufacture",
        character_id=1, character_name="Pilot", quantity=quantity, runs=None,
        estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
    )


# Two distinct products (12345, 22222), both built from the same blueprint and
# sharing material 34 -- needed so the two-job ordering tests below actually
# exercise a shared material rather than silently resolving no materials for
# the second job.
BLUEPRINTS = {
    999: {"manufacturing": {
        "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 100}],
        "products": [{"type_id": 12345, "quantity": 1}, {"type_id": 22222, "quantity": 1}],
    }}
}
DEPTH = {34: {"vwap_5d": 5.0}}


def _build(actions, assets, blueprints=BLUEPRINTS, resolver=None):
    return ShoppingListBuilder().build(
        assigned_actions=actions, corp_assets=assets, market_depth_cache=DEPTH,
        admin_settings=_Admin(), blueprint_data=blueprints,
        meta_resolver=resolver or _NoBlueprints(),
    )


def test_material_quantity_scales_with_runs():
    items = _build([_action(runs=20)], [])
    assert len(items) == 1
    assert items[0].quantity == 2000  # 100 per run x 20 runs


def test_runs_of_none_is_treated_as_one_run():
    items = _build([_action(runs=None)], [])
    assert items[0].quantity == 100


def test_corp_stock_reduces_the_purchase():
    stock = SimpleNamespace(type_id=34, quantity=500, is_blueprint_copy=False)
    items = _build([_action(runs=20)], [stock])
    assert items[0].quantity == 1500


def test_stock_consumed_by_the_first_job_is_not_offered_to_the_second():
    # 1000 in stock; two jobs each needing 1000 => buy 0 then 1000.
    stock = SimpleNamespace(type_id=34, quantity=1000, is_blueprint_copy=False)
    items = _build([_action(runs=10), _action(runs=10, type_id=22222)], [stock])
    assert sum(i.quantity for i in items) == 1000


def test_stock_partially_covering_first_job_is_not_reoffered_to_second():
    # 600 in stock; two jobs each need 1000 (100/run x 10 runs). A correct
    # implementation allocates 600 to job 1 (net 400) and 0 to job 2 (net
    # 1000), for a total of 1400. An implementation that updates
    # already_allocated AFTER the stock-covered continue -- or that never
    # hits an early continue at all here since neither job is fully covered
    # -- would still update already_allocated using the full qty_needed, but
    # the bug in Finding 8 specifically re-offers stock consumed by job 1 to
    # job 2 by allocating in the wrong order/place. To make a wrong ordering
    # visibly wrong (not coincidentally right), use two jobs sharing one
    # material where stock partially covers only the first: if the second
    # job's net_required is computed before the first job's consumption is
    # recorded, both jobs would see the full 600 available and the total
    # would be too low (2000 - 600 - 600 = 800 instead of the correct 1400).
    stock = SimpleNamespace(type_id=34, quantity=600, is_blueprint_copy=False)
    items = _build([_action(runs=10), _action(runs=10, type_id=22222)], [stock])
    assert sum(i.quantity for i in items) == 1400


def test_fully_stocked_material_is_not_listed_at_all():
    stock = SimpleNamespace(type_id=34, quantity=10_000, is_blueprint_copy=False)
    assert _build([_action(runs=20)], [stock]) == []


def test_blueprints_do_not_count_as_material_stock():
    blueprint = SimpleNamespace(type_id=34, quantity=10_000, is_blueprint_copy=False)
    items = _build([_action(runs=20)], [blueprint], resolver=_BlueprintsAre(34))
    assert items[0].quantity == 2000


def test_estimated_total_matches_quantity_times_unit_price():
    items = _build([_action(runs=20)], [])
    assert items[0].estimated_total == items[0].quantity * items[0].estimated_unit_price


# --- sub_manufacture: runs=None, quantity carries the target component count.
# character_assigner.py:229-237 never sets `runs` for these -- a builder that
# reads action.runs directly (the pre-fix behaviour) sees `None`, floors it to
# 1, and buys materials for a single run no matter how many units are needed.


def test_sub_manufacture_buys_materials_for_computed_runs_one_unit_per_run():
    # Blueprint produces 1 unit/run (BLUEPRINTS' default), needs 100 material/run.
    # quantity=50 => 50 runs needed => 5000 material.
    # A wrong implementation that reads action.runs (None) directly floors to
    # 1 run and would buy only 100 -- visibly different from 5000.
    items = _build([_sub_manufacture_action(quantity=50)], [])
    assert len(items) == 1
    assert items[0].quantity == 5000


SUB_MFG_BLUEPRINTS_10_PER_RUN = {
    999: {"manufacturing": {
        "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 100}],
        "products": [{"type_id": 12345, "quantity": 10}],
    }}
}


def test_sub_manufacture_buys_materials_for_computed_runs_ten_units_per_run():
    # Blueprint produces 10 units/run, needs 100 material/run.
    # quantity=50 => ceil(50/10) = 5 runs => 500 material, not 50 (runs=1) and
    # not 5000 (mistaking quantity itself for run count).
    items = _build(
        [_sub_manufacture_action(quantity=50)], [],
        blueprints=SUB_MFG_BLUEPRINTS_10_PER_RUN,
    )
    assert len(items) == 1
    assert items[0].quantity == 500


SUB_MFG_BLUEPRINTS_ZERO_OUTPUT = {
    999: {"manufacturing": {
        "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 100}],
        "products": [{"type_id": 12345, "quantity": 0}],
    }}
}


def test_material_with_no_market_price_is_skipped_not_priced_at_zero():
    """A material absent from market_depth_cache (no vwap_5d, no spot price)
    must be skipped from the shopping list entirely -- not appended with a
    price of 0.0, which would silently make it look free."""
    items = ShoppingListBuilder().build(
        assigned_actions=[_action(runs=20)], corp_assets=[], market_depth_cache={},
        admin_settings=_Admin(), blueprint_data=BLUEPRINTS,
        meta_resolver=_NoBlueprints(),
    )
    assert items == []


def test_sub_manufacture_with_no_usable_per_run_output_falls_back_to_one_run():
    # per_run_output is 0 (incomplete blueprint data) -- must not divide by
    # zero or raise, and must fall back to 1 run (100 material), not crash.
    items = _build(
        [_sub_manufacture_action(quantity=50)], [],
        blueprints=SUB_MFG_BLUEPRINTS_ZERO_OUTPUT,
    )
    assert len(items) == 1
    assert items[0].quantity == 100


# --- invention inputs ----------------------------------------------------------
# Real SDE shape (verified against eve_sde.db, e.g. Damage Control I Blueprint
# 2047 invents into Damage Control II Blueprint 2049, which makes Damage
# Control II 2048): the invention activity lives on the T1 *source* blueprint,
# its products are the T2 blueprints it invents into, and the T2 blueprint has
# no invention activity of its own. The invent action carries the T2 product.

import logging  # noqa: E402

DATACORE_ID = 20416
INVENTION_BLUEPRINTS = {
    # T1 source blueprint: invents into T2 blueprint 1999.
    998: {
        "manufacturing": {"materials": [], "products": [{"type_id": 12344, "quantity": 1}]},
        "invention": {
            "materials": [
                {"type_id": DATACORE_ID, "type_name": "Datacore - Nanite Engineering", "quantity": 2},
            ],
            "products": [{"type_id": 1999, "quantity": 10}],
        },
    },
    # T2 blueprint: manufactures the T2 product 12345, no invention activity.
    1999: {
        "manufacturing": {"materials": [], "products": [{"type_id": 12345, "quantity": 1}]},
        "invention": {"materials": [], "products": []},
    },
}
INVENTION_DEPTH = {DATACORE_ID: {"vwap_5d": 100_000.0}}


def _invent_action(type_id=12345, runs=None):
    # character_assigner builds invent actions with the T2 product's type_id
    # and runs=None.
    return AssignedAction(
        type_id=type_id, type_name="Widget II", action_type="invent",
        character_id=1, character_name="Pilot", quantity=None, runs=runs,
        estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
    )


def _build_invention(actions, assets=(), blueprints=INVENTION_BLUEPRINTS, depth=INVENTION_DEPTH):
    return ShoppingListBuilder().build(
        assigned_actions=actions, corp_assets=list(assets), market_depth_cache=depth,
        admin_settings=_Admin(), blueprint_data=blueprints,
        meta_resolver=_NoBlueprints(),
    )


def test_invention_inputs_come_from_the_t1_source_blueprint():
    items = _build_invention([_invent_action()])
    datacores = [i for i in items if i.type_id == DATACORE_ID]
    assert len(datacores) == 1
    assert datacores[0].quantity == 2  # one attempt
    assert datacores[0].shopping_category == "invention_input"
    assert datacores[0].type_name == "Datacore - Nanite Engineering"
    assert datacores[0].estimated_total == 200_000.0


def test_invention_inputs_scale_with_attempts_when_runs_is_set():
    items = _build_invention([_invent_action(runs=3)])
    assert [i.quantity for i in items if i.type_id == DATACORE_ID] == [6]


def test_datacores_in_stock_are_not_bought_again():
    stock = SimpleNamespace(type_id=DATACORE_ID, quantity=10, is_blueprint_copy=False)
    items = _build_invention([_invent_action()], assets=[stock])
    assert [i for i in items if i.type_id == DATACORE_ID] == []


def test_datacore_stock_is_allocated_across_invention_jobs():
    # 3 in stock, two attempts each needing 2 => buy 0 then 1.
    stock = SimpleNamespace(type_id=DATACORE_ID, quantity=3, is_blueprint_copy=False)
    items = _build_invention([_invent_action(), _invent_action()], assets=[stock])
    assert sum(i.quantity for i in items if i.type_id == DATACORE_ID) == 1


def test_invent_action_without_a_t2_blueprint_is_skipped_with_a_warning(caplog):
    with caplog.at_level(logging.WARNING):
        items = _build_invention([_invent_action(type_id=404040)])
    assert items == []
    assert any(
        "404040" in r.getMessage() and "no blueprint manufactures" in r.getMessage()
        for r in caplog.records if r.levelno == logging.WARNING
    )


def test_invent_action_without_a_t1_source_is_skipped_with_a_warning(caplog):
    only_t2 = {1999: INVENTION_BLUEPRINTS[1999]}
    with caplog.at_level(logging.WARNING):
        items = _build_invention([_invent_action()], blueprints=only_t2)
    assert items == []
    assert any(
        "12345" in r.getMessage() and "no T1 source blueprint" in r.getMessage()
        for r in caplog.records if r.levelno == logging.WARNING
    )


def test_invent_action_whose_source_has_no_invention_materials_is_skipped_with_a_warning(caplog):
    blueprints = {
        998: {**INVENTION_BLUEPRINTS[998],
              "invention": {"materials": [], "products": [{"type_id": 1999, "quantity": 10}]}},
        1999: INVENTION_BLUEPRINTS[1999],
    }
    with caplog.at_level(logging.WARNING):
        items = _build_invention([_invent_action()], blueprints=blueprints)
    assert items == []
    assert any(
        "12345" in r.getMessage() and "no invention materials" in r.getMessage()
        for r in caplog.records if r.levelno == logging.WARNING
    )


def test_invention_input_without_a_price_is_skipped_with_a_warning(caplog):
    with caplog.at_level(logging.WARNING):
        items = _build_invention([_invent_action()], depth={})
    assert items == []
    assert any(
        str(DATACORE_ID) in r.getMessage() and "no price" in r.getMessage()
        for r in caplog.records if r.levelno == logging.WARNING
    )
