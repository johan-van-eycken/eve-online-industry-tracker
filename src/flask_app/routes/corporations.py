from __future__ import annotations

from flask import Blueprint

from flask_app.bootstrap import require_ready
from flask_app.deps import get_state
from flask_app.http import ok

from eve_online_industry_tracker.application.corporations.service import CorporationsService


corporations_bp = Blueprint("corporations", __name__)


@corporations_bp.get("/corporations")
def corporations():
    require_ready(get_state())
    svc = CorporationsService(state=get_state())
    return ok(data=svc.list_corporations())


@corporations_bp.get("/corporations/assets")
def corporations_assets():
    require_ready(get_state())
    svc = CorporationsService(state=get_state())
    return ok(data=svc.list_assets())
