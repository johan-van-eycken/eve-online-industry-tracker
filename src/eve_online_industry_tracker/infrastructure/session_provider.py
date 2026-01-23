from __future__ import annotations

from typing import Any, Protocol


class SessionProvider(Protocol):
    def app_session(self) -> Any: ...

    def sde_session(self) -> Any: ...

    def oauth_session(self) -> Any: ...


class StateSessionProvider:
    """Session provider backed by the injected app state.

    This keeps eve_online_industry_tracker independent of the legacy flask_app module.
    """

    def __init__(self, *, state: Any):
        self._state = state

    def app_session(self) -> Any:
        db = getattr(self._state, "db_app", None)
        if db is None:
            raise RuntimeError("db_app not initialized")
        return db.Session()

    def sde_session(self) -> Any:
        db = getattr(self._state, "db_sde", None)
        if db is None:
            raise RuntimeError("db_sde not initialized")
        return db.Session()

    def oauth_session(self) -> Any:
        db = getattr(self._state, "db_oauth", None)
        if db is None:
            raise RuntimeError("db_oauth not initialized")
        return db.Session()
