from __future__ import annotations

import logging

from flask import Blueprint

from flask_app.bootstrap import require_ready
from flask_app.state import state
from flask_app.http import ok, error


corporations_bp = Blueprint("corporations", __name__)


@corporations_bp.get("/corporations")
def corporations():
    try:
        require_ready()
        corporations_data = state.corp_manager.get_corporations()
        return ok(data=corporations_data)
    except Exception as e:
        logging.error("Error fetching corporations: %s", e)
        return error(message="Error in GET Method `/corporations`: " + str(e))


@corporations_bp.get("/corporations/assets")
def corporations_assets():
    try:
        require_ready()
        assets = state.corp_manager.get_assets()
        return ok(data=assets)
    except Exception as e:
        logging.error("Error fetching corporation assets: %s", e)
        return error(message="Error in GET Method `/corporations/assets`: " + str(e))
