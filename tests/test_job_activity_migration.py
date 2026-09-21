from __future__ import annotations

from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel


def test_the_model_has_an_activity_id_column():
    assert "activity_id" in CorporationIndustryJobsModel.__table__.columns


def test_activity_id_is_queryable(app_session):
    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=1, activity_id=1, product_type_id=12345,
        runs=10, status="active",
    ))
    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=2, activity_id=4, blueprint_type_id=999,
        runs=1, status="active",
    ))
    app_session.commit()

    mfg = app_session.query(CorporationIndustryJobsModel).filter_by(activity_id=1).all()
    assert len(mfg) == 1
    assert mfg[0].product_type_id == 12345


def test_backfill_populates_activity_id_from_raw(app_session):
    from eve_online_industry_tracker.infrastructure.schema_migrations import (
        backfill_job_activity_ids,
    )

    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=3, activity_id=None, raw={"activity_id": 5},
    ))
    app_session.commit()

    updated = backfill_job_activity_ids(app_session)

    assert updated == 1
    row = app_session.query(CorporationIndustryJobsModel).filter_by(job_id=3).one()
    assert row.activity_id == 5


def test_backfill_leaves_existing_values_alone(app_session):
    from eve_online_industry_tracker.infrastructure.schema_migrations import (
        backfill_job_activity_ids,
    )

    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=4, activity_id=1, raw={"activity_id": 8},
    ))
    app_session.commit()

    backfill_job_activity_ids(app_session)

    row = app_session.query(CorporationIndustryJobsModel).filter_by(job_id=4).one()
    assert row.activity_id == 1
