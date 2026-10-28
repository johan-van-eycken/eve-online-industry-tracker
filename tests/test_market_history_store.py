"""Storing a type's market history is one upsert on (type_id, region_id, date):
a re-store updates the day, and a row another writer (the depth collector or a
second overview worker) stored in the meantime is updated, not an IntegrityError."""
from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import create_engine, event, insert
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
from eve_online_industry_tracker.infrastructure.models import BaseApp, MarketHistoryModel

TYPE, REGION = 34, 10000002


def _row(day: str, average: float) -> dict:
    return {"date": day, "average": average, "highest": average, "lowest": average, "volume": 100, "order_count": 9}


def _service(factory, rows):
    esi = SimpleNamespace(get_market_history=lambda type_ids, region_id=None: {TYPE: rows})
    return MarketHistoryService(state=SimpleNamespace(esi_service=esi), sessions=SimpleNamespace(app_session=factory))


def _stored(factory):
    s = factory()
    try:
        return sorted((r.date, r.close) for r in s.query(MarketHistoryModel).all())
    finally:
        s.close()


def test_a_second_store_updates_the_days_instead_of_adding_rows(app_engine):
    factory = sessionmaker(bind=app_engine, future=True)
    _service(factory, [_row("2026-10-01", 5.0), _row("2026-10-02", 6.0)]).fetch_and_store_history(type_id=TYPE, region_id=REGION)
    _service(factory, [_row("2026-10-02", 7.0), _row("2026-10-03", 8.0)]).fetch_and_store_history(type_id=TYPE, region_id=REGION)
    assert _stored(factory) == [("2026-10-01", 5.0), ("2026-10-02", 7.0), ("2026-10-03", 8.0)]


def test_a_day_another_writer_stored_mid_store_is_updated_not_a_failure(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'app.db'}", future=True)
    BaseApp.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")
    other_writer = create_engine(f"sqlite:///{tmp_path / 'app.db'}", future=True)
    fired = []

    @event.listens_for(engine, "before_cursor_execute")
    def _other_writer_commits_first(conn, cursor, statement, parameters, context, executemany):
        # Just before our first write, the other writer stores the same day and commits.
        if not fired and statement.lstrip().upper().startswith("INSERT INTO MARKET_HISTORY"):
            fired.append(1)
            with other_writer.begin() as other:
                other.execute(insert(MarketHistoryModel).values(
                    type_id=TYPE, region_id=REGION, date="2026-10-01", close=1.0, high=1.0, low=1.0,
                    volume=1, order_count=1))

    factory = sessionmaker(bind=engine, future=True)
    assert _service(factory, [_row("2026-10-01", 5.0)]).fetch_and_store_history(type_id=TYPE, region_id=REGION) is None
    assert fired and _stored(factory) == [("2026-10-01", 5.0)]
    engine.dispose()
    other_writer.dispose()
