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


# --- Task 2: chance from the best inventor -------------------------------------

_HEP, _CSE, _CALDARI_ENC, _CAP_CONSTRUCTION = 11433, 11454, 21790, 22242
_ENCRYPTION_IDS = frozenset({3408, 21790, 21791, 23087, 23121, 52308, 55025})


def _skill(type_id, group_id=im.SCIENCE_SKILL_GROUP_ID):
    return {"type_id": type_id, "type_name": f"skill {type_id}", "group_id": group_id}


_BANTAM_LIKE = [_skill(_HEP), _skill(_CSE), _skill(_CALDARI_ENC)]
_NO_ENCRYPTION_BP = [_skill(_HEP), _skill(_CSE)]  # the SDE's Exotic Plasma blueprints


def test_two_science_skills_and_the_encryption_skill_are_identified():
    skill_set, reason = im.invention_skill_set(_BANTAM_LIKE, _ENCRYPTION_IDS)
    assert reason is None
    assert skill_set == im.InventionSkillSet((_HEP, _CSE), _CALDARI_ENC)


def test_a_production_prerequisite_is_not_a_science_skill():
    entries = [*_BANTAM_LIKE, _skill(_CAP_CONSTRUCTION, group_id=268)]
    skill_set, reason = im.invention_skill_set(entries, _ENCRYPTION_IDS)
    assert reason is None and skill_set.science_skill_type_ids == (_HEP, _CSE)


def test_a_blueprint_without_an_encryption_skill_has_a_science_only_skill_set():
    """Amendment A3: 6 SDE blueprints list two science skills and no
    encryption skill. EVE's formula then has no encryption term."""
    skill_set, reason = im.invention_skill_set(_NO_ENCRYPTION_BP, _ENCRYPTION_IDS)
    assert reason is None
    assert skill_set == im.InventionSkillSet((_HEP, _CSE), None)


@pytest.mark.parametrize(("entries", "encryption", "fragment"), [
    ([], _ENCRYPTION_IDS, "lists no invention skills"),
    (_BANTAM_LIKE, frozenset(), "no Encryption Methods skills"),
    ([_skill(_HEP)], _ENCRYPTION_IDS, "found 1 + 0"),
    ([*_BANTAM_LIKE, _skill(21791)], _ENCRYPTION_IDS, "found 2 + 2"),
    ([{"type_id": _HEP}, {"type_id": _CSE}, {"type_id": _CALDARI_ENC}], _ENCRYPTION_IDS, "found 0 + 1"),
])
def test_unidentifiable_invention_skills_name_the_reason(entries, encryption, fragment):
    skill_set, reason = im.invention_skill_set(entries, encryption)
    assert skill_set is None and fragment in reason


def test_the_skill_factor_is_eves_formula():
    skill_set = im.InventionSkillSet((_HEP, _CSE), _CALDARI_ENC)
    assert im.invention_skill_factor(skill_set, {_HEP: 4, _CSE: 4, _CALDARI_ENC: 4}) == pytest.approx(1 + 8 / 30 + 4 / 40)
    # A skill absent from a loaded list is not injected: level 0.
    assert im.invention_skill_factor(skill_set, {}) == 1.0


def test_the_skill_factor_without_an_encryption_skill_is_the_science_term_only():
    skill_set = im.InventionSkillSet((_HEP, _CSE), None)
    assert im.invention_skill_factor(skill_set, {_HEP: 4, _CSE: 4, _CALDARI_ENC: 5}) == pytest.approx(1 + 8 / 30)


def test_the_best_inventor_has_the_highest_factor_and_ties_go_to_the_lowest_id():
    skill_set = im.InventionSkillSet((_HEP, _CSE), _CALDARI_ENC)
    levels = {
        7: {_HEP: 4, _CSE: 4, _CALDARI_ENC: 4},   # 1.3667
        3: {_HEP: 5, _CSE: 5, _CALDARI_ENC: 3},   # 1.4083
        2: {_HEP: 5, _CSE: 5, _CALDARI_ENC: 3},   # tie with 3
    }
    choice = im.best_inventor(skill_set, levels)
    assert choice.character_id == 2
    assert choice.skill_factor == pytest.approx(1 + 10 / 30 + 3 / 40)
    assert im.best_inventor(skill_set, {}).unknown_reason == im.NO_CHARACTER_SKILLS


def _context(levels):
    return im.InventionContext(inventor_skill_levels_by_character_id=levels,
                               encryption_skill_type_ids=_ENCRYPTION_IDS)


def test_the_chance_multiplies_base_skills_and_decryptor_and_caps_at_one():
    context = _context({1: {_HEP: 5, _CSE: 5, _CALDARI_ENC: 5}})
    decryptor = im.DecryptorOption(34202, "Attainment Decryptor", 1.8, -1, 4, 4)
    chance = im.resolve_invention_chance(
        context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
        base_probability=0.34, decryptor=decryptor,
    )
    factor = 1 + 10 / 30 + 5 / 40
    assert chance.skill_factor == pytest.approx(factor)
    assert chance.probability == pytest.approx(0.34 * factor * 1.8)
    assert chance.inventor_character_id == 1 and chance.skills_unknown_reason is None
    assert chance.note is None
    capped = im.resolve_invention_chance(
        context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
        base_probability=0.9, decryptor=decryptor,
    )
    assert capped.probability == 1.0


def test_a_blueprint_without_an_encryption_skill_uses_the_science_term_and_notes_it():
    context = _context({1: {_HEP: 4, _CSE: 4, _CALDARI_ENC: 5}})
    chance = im.resolve_invention_chance(
        context, blueprint_type_id=48098, skill_entries=_NO_ENCRYPTION_BP,
        base_probability=0.34, decryptor=im.NO_DECRYPTOR,
    )
    assert chance.skill_factor == pytest.approx(1 + 8 / 30)
    assert chance.probability == pytest.approx(0.34 * (1 + 8 / 30))
    assert chance.skills_unknown_reason is None
    assert chance.note == im.NO_ENCRYPTION_SKILL_NOTE
    payload = im.invention_chance_payload(chance, effective_probability=chance.probability)
    assert payload["encryption_skill_type_id"] is None
    assert payload["note"] == im.NO_ENCRYPTION_SKILL_NOTE


def test_unknown_skills_use_the_base_chance_and_warn_once_per_reason(caplog):
    context = _context({})
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            chance = im.resolve_invention_chance(
                context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
                base_probability=0.34, decryptor=im.NO_DECRYPTOR,
            )
    assert chance.probability == pytest.approx(0.34)
    assert chance.skill_factor is None
    assert chance.skills_unknown_reason == im.NO_CHARACTER_SKILLS
    assert sum(im.NO_CHARACTER_SKILLS in r.getMessage() for r in caplog.records) == 1


def test_a_quiet_evaluation_does_not_use_up_the_one_warning(caplog):
    context = _context({})
    with caplog.at_level(logging.WARNING):
        with im.quiet_warnings():
            im.resolve_invention_chance(context, blueprint_type_id=1, skill_entries=_BANTAM_LIKE,
                                        base_probability=0.3, decryptor=im.NO_DECRYPTOR)
        im.resolve_invention_chance(context, blueprint_type_id=1, skill_entries=_BANTAM_LIKE,
                                    base_probability=0.3, decryptor=im.NO_DECRYPTOR)
    assert sum(im.NO_CHARACTER_SKILLS in r.getMessage() for r in caplog.records) == 1


def test_warn_once_is_safe_across_worker_threads(caplog):
    from concurrent.futures import ThreadPoolExecutor

    context = _context({})
    with caplog.at_level(logging.WARNING):
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: context.warn_once("k", "once %s", "only"), range(64)))
    assert sum(r.getMessage() == "once only" for r in caplog.records) == 1


def test_the_chance_payload_names_skills_and_the_floored_chance():
    context = _context({5: {_HEP: 1}})
    chance = im.resolve_invention_chance(context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
                                         base_probability=0.3, decryptor=im.NO_DECRYPTOR)
    payload = im.invention_chance_payload(chance, effective_probability=0.31)
    assert payload["science_skill_type_ids"] == [_HEP, _CSE]
    assert payload["encryption_skill_type_id"] == _CALDARI_ENC
    assert payload["inventor_character_id"] == 5
    assert payload["effective_probability"] == 0.31
    assert payload["skills_unknown_reason"] is None
    assert payload["note"] is None
