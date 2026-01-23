import logging
import multiprocessing
import subprocess
import time

import sys
from pathlib import Path

import requests


# Ensure `src/` is on sys.path so `flask_app` and the package can be imported
# when running `python main.py` from the repo root.
_ROOT = Path(__file__).resolve().parent
_SRC = _ROOT / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from flask_app.settings import (
    flask_debug,
    flask_host,
    flask_port,
    health_poll_timeout_seconds,
    health_request_timeout_seconds,
    refresh_metadata_on_startup,
)
from utils.logging_setup import configure_logging


def run_flask():
    """Start the Flask app"""
    from flask_app.app import create_app
    from flask_app.bootstrap import start_background_initialization

    app = create_app()

    # Initialization can be slow (DB/ESI refresh). Start it in the background so
    # the server becomes reachable quickly and /health can report progress.
    start_background_initialization(app_state=app.extensions.get("app_state"), refresh_metadata=refresh_metadata_on_startup())

    # Avoid Werkzeug reloader when running under multiprocessing.
    app.run(
        host=flask_host(),
        port=flask_port(),
        debug=flask_debug(),
        use_reloader=False,
    )


def run_streamlit():
    """Start the Streamlit app in a subprocess"""
    return subprocess.Popen(["streamlit", "run", "streamlit_app.py"])


def wait_for_flask_ready(flask_proc: multiprocessing.Process | None = None, timeout=120):
    """Wait for Flask to become ready"""
    start = time.time()
    while time.time() - start < timeout:
        if flask_proc is not None and not flask_proc.is_alive():
            raise RuntimeError(
                f"Flask process exited early (exitcode={flask_proc.exitcode})."
            )
        try:
            r = requests.get(
                f"http://{flask_host()}:{flask_port()}/health",
                timeout=health_request_timeout_seconds(),
            )

            # Flask reachable: either ready (200) or still initializing (503).
            if r.status_code == 200:
                payload = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                if payload.get("status") == "OK":
                    logging.info("Flask is ready!")
                    return True
            elif r.status_code == 503:
                try:
                    payload = r.json()
                    logging.info(
                        "Flask not ready yet: %s",
                        payload.get("init_state", "unknown"),
                    )
                except Exception:
                    logging.info("Flask not ready yet (503)")
        except requests.RequestException as e:
            logging.debug("Flask not reachable yet: %s", e)
        time.sleep(1)
    raise RuntimeError("Flask did not become ready in time.")


def main():
    configure_logging(default_level="DEBUG")

    flask_proc = None
    streamlit_proc = None

    try:
        # Start Flask first
        logging.info("Starting Flask app...")
        flask_proc = multiprocessing.Process(target=run_flask)
        flask_proc.start()

        logging.info("Waiting for Flask to become ready...")
        wait_for_flask_ready(flask_proc=flask_proc, timeout=health_poll_timeout_seconds())

        # Only start Streamlit after Flask is fully ready
        logging.info("Starting Streamlit app...")
        streamlit_proc = run_streamlit()

        # Main loop: monitor and restart Flask as needed
        while True:
            time.sleep(5)

            # Check if Flask died
            if not flask_proc.is_alive():
                logging.warning("Flask process died unexpectedly. Restarting...")

                # Restart Flask
                flask_proc = multiprocessing.Process(target=run_flask)
                flask_proc.start()

                logging.info("Waiting for Flask to become ready...")
                wait_for_flask_ready(flask_proc=flask_proc, timeout=health_poll_timeout_seconds())

            # Check if Streamlit died (optional)
            if streamlit_proc.poll() is not None:
                logging.warning("Streamlit process died. Restarting...")
                streamlit_proc = run_streamlit()

    except KeyboardInterrupt:
        logging.info("Interrupt received, shutting down...")

        # Shutdown Flask gracefully
        try:
            requests.post(
                f"http://{flask_host()}:{flask_port()}/shutdown",
                timeout=0.5,
            )
            logging.info("Flask shutdown signal sent.")
        except Exception as e:
            logging.warning(f"Could not shutdown Flask gracefully: {e}")

        # Terminate Flask process if still alive
        if flask_proc and flask_proc.is_alive():
            logging.info("Terminating Flask process...")
            flask_proc.terminate()
            flask_proc.join(timeout=5)
            if flask_proc.is_alive():
                logging.warning("Flask did not terminate, killing...")
                flask_proc.kill()

        # Terminate Streamlit process
        if streamlit_proc and streamlit_proc.poll() is None:
            logging.info("Terminating Streamlit process...")
            streamlit_proc.terminate()
            try:
                streamlit_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning("Streamlit did not terminate, killing...")
                streamlit_proc.kill()

        logging.info("Shutdown complete.")

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        # Cleanup on error
        if flask_proc and flask_proc.is_alive():
            flask_proc.terminate()
        if streamlit_proc and streamlit_proc.poll() is None:
            streamlit_proc.terminate()


if __name__ == "__main__":
    main()