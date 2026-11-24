"""Audit 2026-11-24, industry group: one default profile per character (10),
and an overview's decryptor choices land in its own cache dict (12)."""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.infrastructure.models import IndustryProfilesModel
from eve_online_industry_tracker.infrastructure.persistence import industry_profiles_repo as repo


def test_creating_a_default_profile_clears_the_old_default(app_session):
    first = repo.create(app_session, {"character_id": 7, "profile_name": "Raitaru", "is_default": True})
    second = repo.create(app_session, {"character_id": 7, "profile_name": "Azbel", "is_default": True})
    other = repo.create(app_session, {"character_id": 8, "profile_name": "Other", "is_default": True})
    defaults = {p.id for p in app_session.query(IndustryProfilesModel).filter(IndustryProfilesModel.is_default)}
    assert defaults == {second, other}
    assert first not in defaults


def test_decryptor_choices_go_into_the_runs_own_cache_dict():
    state = SimpleNamespace(_overview_result_cache={"hash": "newer run"})
    service = IndustryService(state=state, sessions=None)
    mine = {"hash": "this run"}
    service.publish_decryptor_choices({}, cache=mine)
    assert "decryptor_by_blueprint_type_id" in mine
    assert "decryptor_by_blueprint_type_id" not in state._overview_result_cache
