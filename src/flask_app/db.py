from __future__ import annotations

from typing import Optional

from flask import g, has_app_context

from flask_app.deps import get_state


def _get_or_create_session(g_key: str, session_factory) -> object:
    session = getattr(g, g_key, None)
    if session is None:
        session = session_factory()
        setattr(g, g_key, session)
    return session


def get_db_app_session():
    s = get_state()
    if s.db_app is None:
        raise RuntimeError("App DB not initialized")
    if not has_app_context():
        return s.db_app.session
    return _get_or_create_session("_db_app_session", s.db_app.Session)


def get_db_sde_session():
    s = get_state()
    if s.db_sde is None:
        raise RuntimeError("SDE DB not initialized")
    if not has_app_context():
        return s.db_sde.session
    return _get_or_create_session("_db_sde_session", s.db_sde.Session)


def get_db_oauth_session():
    s = get_state()
    if s.db_oauth is None:
        raise RuntimeError("OAuth DB not initialized")
    if not has_app_context():
        return s.db_oauth.session
    return _get_or_create_session("_db_oauth_session", s.db_oauth.Session)


def close_request_sessions(exc: Optional[BaseException] = None) -> None:
    # Best-effort close; errors shouldn't mask the original exception.
    for key in ("_db_app_session", "_db_sde_session", "_db_oauth_session"):
        session = getattr(g, key, None)
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
            try:
                delattr(g, key)
            except Exception:
                pass

    # If DatabaseManager.session is a scoped_session proxy, remove it so the next
    # request gets a fresh session.
    s = get_state()
    for db in (s.db_app, s.db_sde, s.db_oauth):
        if db is None:
            continue
        scoped = getattr(db, "session", None)
        remove = getattr(scoped, "remove", None)
        if callable(remove):
            try:
                remove()
            except Exception:
                pass
