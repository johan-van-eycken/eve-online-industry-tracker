from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.models import BaseApp  # noqa: E402
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


class _EngineSessionProvider:
    """Session provider that mirrors production's StateSessionProvider.

    Each app_session() call returns a fresh Session bound to the shared
    in-memory engine, and close() is real. sqlite:// in-memory engines use
    SingletonThreadPool, so every session shares the one underlying
    connection and sees the same schema and committed data -- there is no
    need to keep a single Session alive across calls to get that.
    """

    def __init__(self, engine) -> None:
        self._factory = sessionmaker(bind=engine, future=True)

    def app_session(self) -> Session:
        return self._factory()


@pytest.fixture()
def session_provider(app_engine) -> _EngineSessionProvider:
    return _EngineSessionProvider(app_engine)


@pytest.fixture()
def planner_repo(session_provider) -> DailyPlannerRepository:
    return DailyPlannerRepository(session_provider=session_provider)
