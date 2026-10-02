from __future__ import annotations

from datetime import datetime, timezone

from eve_online_industry_tracker.infrastructure.models import BuildPlanModel


def test_app_session_fixture_has_planner_tables(app_session):
    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    app_session.add(BuildPlanModel(created_at=now, updated_at=now, status="active"))
    app_session.commit()
    assert app_session.query(BuildPlanModel).count() == 1


def test_planner_repo_fixture_is_wired(planner_repo):
    assert planner_repo is not None
