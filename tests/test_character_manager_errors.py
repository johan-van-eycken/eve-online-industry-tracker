"""F1 item 7: CharacterManager.get_characters() raises CharacterDataError
(not a bare Exception) when a character cannot be serialized; the slot
capacities propagate it, and the active jobs' cosmetic name map logs a
WARNING and keeps the job's ID. Fake values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from eve_online_industry_tracker.application.characters.character_manager import (
    CharacterDataError,
    CharacterManager,
)


class _Char:
    def __init__(self, error=None):
        self._error = error

    def get_character(self):
        if self._error:
            raise self._error
        return {"character_id": 90000001, "character_name": "Fake"}


def _manager(*chars):
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    return mgr


def test_get_characters_returns_the_serialized_characters():
    assert _manager(_Char()).get_characters() == [{"character_id": 90000001, "character_name": "Fake"}]


def test_get_characters_without_characters_is_a_value_error():
    with pytest.raises(ValueError):
        _manager().get_characters()


def test_a_db_failure_is_a_character_data_error_naming_it():
    with pytest.raises(CharacterDataError, match="fake db down") as exc_info:
        _manager(_Char(OperationalError("SELECT fake", {}, Exception("fake db down")))).get_characters()
    assert isinstance(exc_info.value.__cause__, OperationalError)
    assert not isinstance(exc_info.value, RuntimeError)     # not read as an ESI error by the ESI catch sites


def test_a_programming_error_propagates_as_is():
    with pytest.raises(AttributeError):
        _manager(_Char(AttributeError("fake bug"))).get_characters()


def test_the_active_jobs_name_map_survives_an_unreadable_character_list(session_provider, app_engine, caplog):
    from eve_online_industry_tracker.application.industry.service import IndustryService
    from eve_online_industry_tracker.infrastructure.models import CharacterIndustryJobsModel

    with Session(app_engine) as session:
        session.add(CharacterIndustryJobsModel(character_id=7, job_id=1, status="active", activity_id=1,
                                               raw={"activity_id": 1}))
        session.commit()

    def _fail():
        raise CharacterDataError("Error retrieving characters: fake")

    state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=_fail), db_sde=SimpleNamespace(language="en"))
    svc = IndustryService(state=state, sessions=session_provider)
    svc._character_slot_capacities = lambda: {}
    with caplog.at_level(logging.WARNING):
        (job,) = svc.industry_active_jobs()["jobs"]
    assert job["character_id"] == 7
    assert "Error retrieving characters: fake" in caplog.text
