from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import create_engine
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
    """
    engine = create_engine("sqlite://", future=True)
    BaseSde.metadata.create_all(engine)
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
