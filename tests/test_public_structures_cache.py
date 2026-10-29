from datetime import datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import update
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.db_models import PublicStructuresModel
from eve_online_industry_tracker.infrastructure.persistence import public_structures_repo
from eve_online_industry_tracker.infrastructure.public_structures_cache_service import (
    get_cached_public_structures,
    refresh_public_structures_for_system,
)


def _structure(structure_id: int, name: str) -> dict:
    return {"structure_id": structure_id, "system_id": 30000142, "structure_name": name}


def _state(app_engine, fetched: list[dict]):
    return SimpleNamespace(
        db_app=SimpleNamespace(Session=sessionmaker(bind=app_engine, future=True)),
        esi_service=SimpleNamespace(get_public_structures=lambda **kw: fetched),
        public_structures_last_refresh_by_system={},
    )


def test_a_system_refresh_keeps_the_structures_the_sampled_scan_did_not_find(app_engine):
    session = sessionmaker(bind=app_engine, future=True)()
    public_structures_repo.upsert_many(session, [_structure(1, "Alpha"), _structure(2, "Beta"), _structure(3, "Gamma")])
    session.close()
    state = _state(app_engine, [{"station_id": 2, "system_id": 30000142, "station_name": "Beta renamed"}])

    assert refresh_public_structures_for_system(state=state, system_id=30000142) == 1

    rows, _ = get_cached_public_structures(state=state, system_id=30000142, ttl_seconds=3600)
    assert sorted((r["station_id"], r["station_name"]) for r in rows) == [(1, "Alpha"), (2, "Beta renamed"), (3, "Gamma")]


def test_the_read_returns_stale_rows_next_to_fresh_ones_and_the_ttl_only_sets_is_fresh(app_engine):
    session = sessionmaker(bind=app_engine, future=True)()
    public_structures_repo.upsert_many(session, [_structure(1, "Old"), _structure(2, "New")])
    session.execute(
        update(PublicStructuresModel)
        .where(PublicStructuresModel.structure_id == 1)
        .values(updated_at=datetime.utcnow() - timedelta(days=2))
    )
    session.commit()
    session.close()
    state = _state(app_engine, [])

    rows, is_fresh = get_cached_public_structures(state=state, system_id=30000142, ttl_seconds=3600)
    assert {r["station_id"] for r in rows} == {1, 2} and is_fresh is True

    session = sessionmaker(bind=app_engine, future=True)()
    session.execute(update(PublicStructuresModel).values(updated_at=datetime.utcnow() - timedelta(days=2)))
    session.commit()
    session.close()
    rows, is_fresh = get_cached_public_structures(state=state, system_id=30000142, ttl_seconds=3600)
    assert {r["station_id"] for r in rows} == {1, 2} and is_fresh is False
