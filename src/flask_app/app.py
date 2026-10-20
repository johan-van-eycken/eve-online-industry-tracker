from __future__ import annotations

import logging

from flask import Flask

from werkzeug.exceptions import HTTPException

from flask_app.db import close_request_sessions
from flask_app.http import error
from flask_app.state import state

from eve_online_industry_tracker.application.characters.character_manager import UnknownCharacterError
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.sde.industry import REIMPORT_HINT, SdeIndustryDataMissing

from flask_app.routes.admin import admin_bp
from flask_app.routes.characters import characters_bp
from flask_app.routes.corporations import corporations_bp
from flask_app.routes.locations import locations_bp
from flask_app.routes.static_data import static_data_bp
from flask_app.routes.industry import industry_bp
from flask_app.routes.settings import settings_bp
from flask_app.routes.daily_planner import daily_planner_bp


def create_app() -> Flask:
    app = Flask(__name__)

    # Expose state via the Flask app instance so routes don't need to import the
    # module-level global directly.
    app.extensions["app_state"] = state

    # Ensure DB sessions created during a request are always closed.
    app.teardown_appcontext(close_request_sessions)

    @app.errorhandler(HTTPException)
    def _handle_http_exception(e: HTTPException):
        return error(message=e.description, status_code=e.code or 500)

    @app.errorhandler(ServiceError)
    def _handle_service_error(e: ServiceError):
        extra = {}
        if e.data is not None:
            extra["data"] = e.data
        if e.meta is not None:
            extra["meta"] = e.meta
        return error(message=e.message, status_code=e.status_code, **extra)

    @app.errorhandler(UnknownCharacterError)
    def _handle_unknown_character(e: UnknownCharacterError):
        # A character_id or name the app does not have: nothing was refreshed.
        logging.warning("%s", e)
        return error(message=str(e), status_code=404, code="unknown_character")

    @app.errorhandler(SdeIndustryDataMissing)
    def _handle_sde_reimport_required(e: SdeIndustryDataMissing):
        # Logged once as an ERROR by infrastructure.sde.industry; no traceback per request.
        return error(message=str(e), status_code=503, code="sde_reimport_required", hint=REIMPORT_HINT)

    @app.errorhandler(RuntimeError)
    def _handle_runtime_error(e: RuntimeError):
        # Common case in this app: initialization has not completed.
        msg = str(e)
        if msg.startswith("Application not ready:"):
            return error(message=msg, status_code=503)
        logging.exception("Unhandled RuntimeError")
        return error(message=msg, status_code=500)

    @app.errorhandler(Exception)
    def _handle_unhandled_exception(e: Exception):
        logging.exception("Unhandled exception")
        return error(message=str(e), status_code=500)

    @app.errorhandler(404)
    def _handle_not_found(_):
        return error(message="Not found", status_code=404)

    # Blueprints
    app.register_blueprint(admin_bp)
    app.register_blueprint(static_data_bp)
    app.register_blueprint(characters_bp)
    app.register_blueprint(corporations_bp)
    app.register_blueprint(locations_bp)
    app.register_blueprint(industry_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(daily_planner_bp)

    return app
