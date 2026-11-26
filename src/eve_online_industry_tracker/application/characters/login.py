"""One EVE SSO login at a time, for one named character, started from the Characters page."""
from __future__ import annotations

import logging
import threading
from typing import Any

from eve_online_industry_tracker.infrastructure.esi_client import ESIClient, WrongCharacterError
from flask_app.restart import request_restart

_lock = threading.Lock()
_status: dict[str, Any] = {"state": "idle", "character_name": None, "message": "", "restarting": False}


def start(cfg: Any, db_oauth: Any, character_name: str, is_corp_director: bool) -> str | None:
    """The login URL, with the login waiting on a daemon thread; None while another login runs."""
    if not _lock.acquire(blocking=False):
        return None
    try:
        client = ESIClient(cfg, db_oauth, character_name, False, is_corp_director, load_tokens=False)
        url = client.authorization_url()
        _status.update(state="waiting", character_name=character_name, restarting=False,
                       message=f"Log in as {character_name} in the browser.")
        threading.Thread(target=_run, args=(client, character_name), daemon=True, name="character-login").start()
    except BaseException:
        _lock.release()
        raise
    return url


def _run(client: ESIClient, wanted: str) -> None:
    try:
        client.register_new_character(expected_name=wanted)
        _status.update(state="done", message=f"{wanted} is activated.", restarting=request_restart())
    except WrongCharacterError as e:
        _status.update(state="wrong_character", message=f"You logged in as {e.got}. Log in as {wanted}.")
    except TimeoutError:
        _status.update(state="timeout", message=f"No login for {wanted} within 5 minutes.")
    except Exception as e:   # the thread's boundary: the page shows the failure
        logging.exception("Login of %s failed", wanted)
        _status.update(state="failed", message=f"Login of {wanted} failed: {e}")
    finally:
        try:
            client.db_oauth.session.remove()   # this thread's session
        finally:
            _lock.release()


def status() -> dict[str, Any]:
    return dict(_status)
