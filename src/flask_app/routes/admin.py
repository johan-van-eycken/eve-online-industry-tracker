from __future__ import annotations

import logging
import os
import signal

from flask import Blueprint, jsonify, request

from flask_app.state import state
from flask_app.http import ok, error


admin_bp = Blueprint("admin", __name__)


@admin_bp.get("/health")
def health_check():
    if state.init_state != "Ready":
        payload = {"status": "not_ready", "init_state": state.init_state}
        if state.init_error:
            payload["error"] = state.init_error
        return jsonify(payload), 503
    return jsonify({"status": "OK"}), 200


@admin_bp.route("/shutdown", methods=["GET", "POST"])
def shutdown():
    """Shutdown the Flask server."""
    try:
        logging.info("Shutdown request received")

        # For Windows
        if os.name == "nt":
            os.kill(os.getpid(), signal.SIGTERM)
        else:
            # For Unix-like systems
            func = request.environ.get("werkzeug.server.shutdown")
            if func is None:
                os.kill(os.getpid(), signal.SIGTERM)
            else:
                func()

        return ok(message="Server shutting down...")
    except Exception as e:
        logging.error("Error during shutdown: %s", e)
        return error(message=str(e))
