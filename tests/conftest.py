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

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


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


class _StaticSessionProvider:
    """Session provider that always hands back the same test session.

    close() is a no-op so repository code that closes its session does not
    invalidate the fixture for the rest of the test.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def app_session(self) -> Session:
        self._session.close = lambda: None  # type: ignore[method-assign]
        return self._session


@pytest.fixture()
def session_provider(app_session) -> _StaticSessionProvider:
    return _StaticSessionProvider(app_session)


@pytest.fixture()
def planner_repo(session_provider) -> DailyPlannerRepository:
    return DailyPlannerRepository(session_provider=session_provider)
