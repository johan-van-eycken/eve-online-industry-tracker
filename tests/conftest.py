from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import Column, Integer, JSON, MetaData, String, Table, create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.models import BaseApp, BaseSde  # noqa: E402
from eve_online_industry_tracker.infrastructure.persistence.daily_planner_repo import (  # noqa: E402
    DailyPlannerRepository,
)


@pytest.fixture()
def app_engine():
    engine = create_engine("sqlite://", future=True)
    BaseApp.metadata.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def app_session(app_engine) -> Session:
    factory = sessionmaker(bind=app_engine, future=True)
    session = factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def sde_engine():
    """A separate in-memory engine with the SDE schema (empty tables).

    TypeMetadataResolver.prefetch() runs real ORM queries (session.query(Types),
    a Table("metaGroups", ..., autoload_with=bind) reflection, etc.) against
    whatever session sde_session() hands back. Reusing app_engine would make
    those queries raise "no such table" -- since Task 5 removed the swallowing
    try/except in prefetch(), that propagates and breaks any test that calls
    _build_input_rows(). Creating the real (if empty) SDE schema instead lets
    prefetch's queries succeed and simply find nothing, which the contract
    already treats as legitimate optional data (meta_group_id/category_id ->
    None, is_blueprint() -> False).

    `metaGroups` is deliberately NOT part of BaseSde.metadata: get_type_data
    (infrastructure/sde/types.py) reaches it via raw reflection --
    `Table("metaGroups", MetaData(), autoload_with=bind)` -- not an ORM model,
    so `BaseSde.metadata.create_all()` never creates it. That is harmless only
    while `Types` is empty: `get_type_data` guards the reflection behind
    `if meta_group_ids:`, so no query ever touches `metaGroups` with zero
    `Types` rows. The first caller that inserts a real `Types` row with a
    non-null `metaGroupID` (a legitimate use of this same fixture -- see
    test_type_metadata.py's real-schema test) hits the reflection and would
    get `NoSuchTableError: metaGroups` with no swallowing try/except to hide
    it. Create the table explicitly, with exactly the columns get_type_data
    selects (id, color, name, iconID), so that path works too.
    """
    engine = create_engine("sqlite://", future=True)
    BaseSde.metadata.create_all(engine)
    meta = MetaData()
    Table(
        "metaGroups",
        meta,
        Column("id", Integer, primary_key=True),
        Column("color", String, nullable=True),
        Column("name", JSON, nullable=True),
        Column("iconID", Integer, nullable=True),
    )
    meta.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()


class _EngineSessionProvider:
    """Session provider that mirrors production's StateSessionProvider.

    Each app_session()/sde_session() call returns a fresh Session bound to
    the relevant shared in-memory engine, and close() is real. sqlite://
    in-memory engines use SingletonThreadPool, so every session shares the
    one underlying connection and sees the same schema and committed data --
    there is no need to keep a single Session alive across calls to get that.
    """

    def __init__(self, engine, sde_engine=None) -> None:
        self._factory = sessionmaker(bind=engine, future=True)
        self._sde_factory = sessionmaker(bind=sde_engine, future=True) if sde_engine is not None else None

    def app_session(self) -> Session:
        return self._factory()

    def sde_session(self) -> Session:
        if self._sde_factory is None:
            raise RuntimeError("session_provider has no sde_engine configured")
        return self._sde_factory()


@pytest.fixture()
def session_provider(app_engine, sde_engine) -> _EngineSessionProvider:
    return _EngineSessionProvider(app_engine, sde_engine)


@pytest.fixture()
def planner_repo(session_provider) -> DailyPlannerRepository:
    return DailyPlannerRepository(session_provider=session_provider)
