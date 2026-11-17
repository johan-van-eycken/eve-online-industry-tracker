"""The job snapshot keeps the ESI blueprint_id as blueprint_item_id.

Corp and character job sync pass the ESI payload as an object
(type("IndustryJobPayload", (), j)()): it has `blueprint_id`, no
`blueprint_item_id` and no `raw`. A research job has no provenance, so
without reading `blueprint_id` the item id was stored NULL and the daily
planner saw the BPO in research as free.
"""
from eve_online_industry_tracker.application.characters.asset_provenance import (
    resolve_industry_job_cost_snapshot,
)


def test_esi_payload_blueprint_id_becomes_blueprint_item_id() -> None:
    esi_job = {
        "job_id": 1, "activity_id": 4, "status": "active", "runs": 1,
        "blueprint_id": 1056309049947, "blueprint_type_id": 31110,
    }
    snapshot = resolve_industry_job_cost_snapshot(
        job=type("IndustryJobPayload", (), esi_job)(),
        sde_session=None,
        market_price_map=None,
        blueprint_provenance=None,
    )
    assert snapshot["blueprint_item_id"] == 1056309049947


def test_backfill_sets_blueprint_item_id_from_raw(app_session) -> None:
    from sqlalchemy import text

    from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel
    from eve_online_industry_tracker.infrastructure.schema_migrations import _backfill_blueprint_item_id_sql

    app_session.add_all([
        CorporationIndustryJobsModel(corporation_id=1, job_id=1, raw={"blueprint_id": 77}),
        CorporationIndustryJobsModel(corporation_id=1, job_id=2, blueprint_item_id=5, raw={"blueprint_id": 88}),
    ])
    app_session.commit()

    class _Db:
        def execute(self, sql: str) -> None:
            app_session.execute(text(sql))
            app_session.commit()

    _backfill_blueprint_item_id_sql(_Db(), table="corporation_industry_jobs")

    by_job = {r.job_id: r.blueprint_item_id for r in app_session.query(CorporationIndustryJobsModel)}
    assert by_job == {1: 77, 2: 5}
