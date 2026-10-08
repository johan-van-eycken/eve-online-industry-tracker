"""EVE invention rules (application/industry/invention_model.py)."""
from __future__ import annotations

import logging

import pytest

from eve_online_industry_tracker.application.industry import invention_model as im


def _option(**overrides):
    base = dict(type_id=34201, type_name="Accelerant Decryptor", probability_multiplier=1.2,
                me_modifier=2, te_modifier=10, max_run_modifier=1)
    base.update(overrides)
    return im.DecryptorOption(**base)


def test_an_invented_bpc_without_a_decryptor_is_me_2_te_4():
    assert im.invented_blueprint_efficiency(im.NO_DECRYPTOR) == (2, 4)


@pytest.mark.parametrize(("me_mod", "te_mod", "expected"), [
    (2, 10, (4, 14)),   # Accelerant
    (-2, 2, (0, 6)),    # Augmentation
    (-1, 4, (1, 8)),    # Attainment
    (3, 6, (5, 10)),    # Process
    (1, -2, (3, 2)),    # Parity
])
def test_decryptor_modifiers_add_to_the_invented_efficiency(me_mod, te_mod, expected):
    assert im.invented_blueprint_efficiency(_option(me_modifier=me_mod, te_modifier=te_mod)) == expected


def test_invented_efficiency_stays_inside_eves_research_range():
    assert im.invented_blueprint_efficiency(_option(me_modifier=20, te_modifier=40)) == (10, 20)
    assert im.invented_blueprint_efficiency(_option(me_modifier=-9, te_modifier=-9)) == (0, 0)


def test_runs_per_invented_bpc_add_the_decryptor_run_modifier():
    assert im.invented_runs_per_blueprint(10, im.NO_DECRYPTOR) == 10
    assert im.invented_runs_per_blueprint(10, _option(max_run_modifier=9)) == 19
    assert im.invented_runs_per_blueprint(1, _option(max_run_modifier=-5)) == 1


def test_options_from_sde_rows_put_no_decryptor_first_then_type_id_order():
    rows = [
        {"type_id": 34203, "type_name": "Augmentation Decryptor", "invention_probability_multiplier": 0.6,
         "invention_me_modifier": -2, "invention_te_modifier": 2, "invention_max_run_modifier": 9},
        {"type_id": 34201, "type_name": "Accelerant Decryptor", "invention_probability_multiplier": 1.2,
         "invention_me_modifier": 2, "invention_te_modifier": 10, "invention_max_run_modifier": 1},
    ]
    options = im.decryptor_options_from_rows(rows)
    assert options[0] is im.NO_DECRYPTOR
    assert [o.type_id for o in options[1:]] == [34201, 34203]
    assert options[2].max_run_modifier == 9 and options[2].probability_multiplier == 0.6


def test_a_malformed_or_non_positive_decryptor_row_is_skipped_with_a_warning(caplog):
    rows = [
        {"type_id": 34201, "type_name": "Broken"},  # no modifiers at all
        {"type_id": 34202, "type_name": "Zero", "invention_probability_multiplier": 0.0,
         "invention_me_modifier": 0, "invention_te_modifier": 0, "invention_max_run_modifier": 0},
    ]
    with caplog.at_level(logging.WARNING):
        assert im.decryptor_options_from_rows(rows) == (im.NO_DECRYPTOR,)
    assert sum("decryptor" in r.getMessage().lower() for r in caplog.records) == 2


def test_quiet_warnings_drops_warnings_but_not_errors(caplog):
    log = logging.getLogger("test_invention_model.quiet")
    log.addFilter(im.QuietWarningsFilter())
    with caplog.at_level(logging.WARNING):
        with im.quiet_warnings():
            assert im.warnings_are_quiet()
            log.warning("hidden")
            log.error("still shown")
        assert not im.warnings_are_quiet()
        log.warning("shown")
    assert [r.getMessage() for r in caplog.records] == ["still shown", "shown"]


def test_the_default_invention_context_plans_without_a_decryptor():
    assert im.InventionContext().decryptor_for_blueprint(9002) is im.NO_DECRYPTOR
