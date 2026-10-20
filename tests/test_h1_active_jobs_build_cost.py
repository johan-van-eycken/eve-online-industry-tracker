"""H1 addition: /industry_active_jobs carries each stored job's build cost and
why it is partly unknown (character and corporation jobs), None when absent.
Fake ids."""
from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy.orm import Session

from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.infrastructure.models import CharacterIndustryJobsModel, CorporationIndustryJobsModel

_FIELDS = ("total_build_cost", "unit_build_cost", "invention_cost_unknown_reason", "materials_cost_lower_bound_reason")


def _jobs(session_provider):
    state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=lambda: []), db_sde=SimpleNamespace(language="en"))
    svc = IndustryService(state=state, sessions=session_provider)
    svc._character_slot_capacities = lambda: {}
    return {job["job_id"]: job for job in svc.industry_active_jobs()["jobs"]}


def test_the_stored_build_cost_and_its_reasons_are_on_each_job(session_provider, app_engine):
    with Session(app_engine) as session:
        session.add(CharacterIndustryJobsModel(
            character_id=7, job_id=1, status="active", activity_id=1, raw={"activity_id": 1},
            total_build_cost=140.0, unit_build_cost=70.0,
            invention_cost_unknown_reason="invention cost unknown: fake",
            materials_cost_lower_bound_reason="materials lower bound: fake",
        ))
        session.add(CorporationIndustryJobsModel(
            corporation_id=98000001, installer_id=7, job_id=2, status="active", activity_id=1,
            raw={"activity_id": 1}, total_build_cost=50.0, unit_build_cost=5.0,
        ))
        session.add(CharacterIndustryJobsModel(character_id=7, job_id=3, status="active", activity_id=1,
                                               raw={"activity_id": 1}))
        session.commit()
    jobs = _jobs(session_provider)
    assert [jobs[1][f] for f in _FIELDS] == [140.0, 70.0, "invention cost unknown: fake", "materials lower bound: fake"]
    assert [jobs[2][f] for f in _FIELDS] == [50.0, 5.0, None, None]
    assert [jobs[3][f] for f in _FIELDS] == [None, None, None, None]
