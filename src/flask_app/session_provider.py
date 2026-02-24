from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider
from flask_app.db import get_db_app_session, get_db_oauth_session, get_db_sde_session


class FlaskSessionProvider(SessionProvider):
    """SessionProvider backed by Flask's request-scoped sessions.

    This ensures sessions are closed via `app.teardown_appcontext(close_request_sessions)`.
    """

    def app_session(self) -> Any:
        return get_db_app_session()

    def sde_session(self) -> Any:
        return get_db_sde_session()

    def oauth_session(self) -> Any:
        return get_db_oauth_session()
