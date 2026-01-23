from __future__ import annotations

from flask_app.app import create_app
from flask_app.bootstrap import start_background_initialization
from flask_app.settings import flask_debug, flask_host, flask_port, refresh_metadata_on_startup
from utils.logging_setup import configure_logging


def main() -> None:
    configure_logging(default_level="INFO")

    app = create_app()

    # Initialization can be slow (DB/ESI refresh). Start it in the background so
    # the server becomes reachable quickly and /health can report progress.
    start_background_initialization(app_state=app.extensions.get("app_state"), refresh_metadata=refresh_metadata_on_startup())

    # Avoid Werkzeug reloader to prevent double-starting.
    app.run(host=flask_host(), port=flask_port(), debug=flask_debug(), use_reloader=False)


if __name__ == "__main__":
    main()
