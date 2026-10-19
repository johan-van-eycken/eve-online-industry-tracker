"""The producer hands the realized side its invention inputs: the corp's
inventor skills, the SDE encryption skills, the admin floor and the decryptor it
chose per invented blueprint in the last overview. Fake ids only."""
from __future__ import annotations

from types import MethodType, SimpleNamespace

from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    NO_DECRYPTOR_DATA_REASON,
    DecryptorChoice,
    DecryptorOption,
)
from eve_online_industry_tracker.application.industry.service import IndustryService

_DECRYPTOR = DecryptorOption(type_id=990041, type_name="Fake Decryptor", probability_multiplier=1.2, max_run_modifier=1)


class _Admin:
    def __init__(self, values):
        self._values = values

    def get(self, category, key):
        return self._values.get((category, key))


def _service(*, selection: bool = True, cache=None) -> IndustryService:
    state = SimpleNamespace(
        admin_settings=_Admin({
            ("industry", "invention_decryptor_selection_enabled"): selection,
            ("industry", "invention_probability_floor"): 0.05,
        }),
        char_manager=SimpleNamespace(get_characters=lambda: [
            {"character_id": 7001, "skills": {"skills": [{"skill_id": 990031, "trained_skill_level": 4}]}},
        ]),
    )
    if cache is not None:
        state._overview_result_cache = cache
    service = IndustryService(state=state)
    service._load_encryption_skill_type_ids = MethodType(lambda self: frozenset({990033}), service)
    return service


def test_the_inputs_carry_the_corp_inventors_the_floor_and_the_overviews_decryptors():
    inputs = _service(cache={"decryptor_by_blueprint_type_id": {990002: _DECRYPTOR}}).realized_invention_inputs()
    assert inputs.invention_context.inventor_skill_levels_by_character_id == {7001: {990031: 4}}
    assert inputs.invention_context.encryption_skill_type_ids == frozenset({990033})
    assert inputs.probability_floor == 0.05
    assert inputs.decryptor_for(990002) == _DECRYPTOR
    # Selection on and no overview choice for it: unknown, never "no decryptor".
    assert inputs.decryptor_for(990099) is None


def test_with_decryptor_selection_off_every_blueprint_uses_no_decryptor_like_the_producer():
    inputs = _service(selection=False).realized_invention_inputs()
    assert inputs.decryptor_for(990002) == NO_DECRYPTOR


def test_without_an_overview_no_decryptor_is_known():
    assert _service().realized_invention_inputs().decryptor_for(990002) is None


def test_the_overview_publishes_its_decryptor_choices_but_not_a_no_data_fallback():
    choices = {
        990002: DecryptorChoice(_DECRYPTOR, "chosen"),
        990004: DecryptorChoice(NO_DECRYPTOR, "no decryptor beats the others"),
        990006: DecryptorChoice(NO_DECRYPTOR, NO_DECRYPTOR_DATA_REASON),
    }
    assert IndustryService._published_decryptor_choices(choices) == {990002: _DECRYPTOR, 990004: NO_DECRYPTOR}


def test_the_bootstrap_provider_reads_the_producer_inputs(monkeypatch):
    service = _service(cache={"decryptor_by_blueprint_type_id": {990002: _DECRYPTOR}})
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    ap.set_realized_invention_inputs_provider(service.realized_invention_inputs)
    inputs, reason = ap.current_realized_invention_inputs()
    assert reason is None and inputs.decryptor_for(990002) == _DECRYPTOR
