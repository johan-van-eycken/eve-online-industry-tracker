from __future__ import annotations

from flask import Flask

from werkzeug.exceptions import HTTPException

from flask_app.db import close_request_sessions
from flask_app.http import error

from flask_app.routes.admin import admin_bp
from flask_app.routes.characters import characters_bp
from flask_app.routes.corporations import corporations_bp
from flask_app.routes.locations import locations_bp
from flask_app.routes.static_data import static_data_bp
from flask_app.routes.industry import industry_bp


def create_app() -> Flask:
    app = Flask(__name__)

    # Ensure DB sessions created during a request are always closed.
    app.teardown_appcontext(close_request_sessions)

    @app.errorhandler(HTTPException)
    def _handle_http_exception(e: HTTPException):
        return error(message=e.description, status_code=e.code or 500)

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

    return app
