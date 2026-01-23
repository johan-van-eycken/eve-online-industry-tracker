from __future__ import annotations

import argparse
import logging
import multiprocessing
import subprocess
import time

import requests

from flask_app.app import create_app
from flask_app.bootstrap import start_background_initialization
from flask_app.settings import (
    flask_debug,
    flask_host,
    flask_port,
    health_poll_timeout_seconds,
    health_request_timeout_seconds,
    refresh_metadata_on_startup,
)
from utils.logging_setup import configure_logging


def run_flask() -> None:
    app = create_app()
    start_background_initialization(app_state=app.extensions.get("app_state"), refresh_metadata=refresh_metadata_on_startup())
    app.run(host=flask_host(), port=flask_port(), debug=flask_debug(), use_reloader=False)


def run_streamlit() -> subprocess.Popen:
    return subprocess.Popen(["streamlit", "run", "streamlit_app.py"])


def wait_for_flask_ready(flask_proc: multiprocessing.Process | None = None, timeout: int = 120) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if flask_proc is not None and not flask_proc.is_alive():
            raise RuntimeError(f"Flask process exited early (exitcode={flask_proc.exitcode}).")

        try:
            r = requests.get(
                f"http://{flask_host()}:{flask_port()}/health",
                timeout=health_request_timeout_seconds(),
            )

            if r.status_code == 200:
                payload = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                if payload.get("status") == "OK":
                    logging.info("Flask is ready!")
                    return True
            elif r.status_code == 503:
                try:
                    payload = r.json()
                    logging.info("Flask not ready yet: %s", payload.get("init_state", "unknown"))
                except Exception:
                    logging.info("Flask not ready yet (503)")
        except requests.RequestException as e:
            logging.debug("Flask not reachable yet: %s", e)

        time.sleep(1)

    raise RuntimeError("Flask did not become ready in time.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eve-online-industry-tracker",
        description="Runs the EVE Online Industry Tracker (Flask API + Streamlit UI).",
    )

    parser.add_argument(
        "--log-level",
        default="DEBUG",
        help="Python logging level (default: %(default)s)",
    )

    parser.add_argument(
        "--no-streamlit",
        action="store_true",
        help="Run only the Flask API (do not start Streamlit).",
    )

    parser.add_argument(
        "--health-timeout-seconds",
        type=int,
        default=None,
        help="Override health polling timeout while waiting for Flask readiness.",
    )

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    configure_logging(default_level=str(args.log_level).upper())

    flask_proc: multiprocessing.Process | None = None
    streamlit_proc: subprocess.Popen | None = None

    try:
        logging.info("Starting Flask app...")
        flask_proc = multiprocessing.Process(target=run_flask)
        flask_proc.start()

        logging.info("Waiting for Flask to become ready...")
        wait_timeout = args.health_timeout_seconds or health_poll_timeout_seconds()
        wait_for_flask_ready(flask_proc=flask_proc, timeout=wait_timeout)

        if not args.no_streamlit:
            logging.info("Starting Streamlit app...")
            streamlit_proc = run_streamlit()

        while True:
            time.sleep(5)

            if not flask_proc.is_alive():
                logging.warning("Flask process died unexpectedly. Restarting...")
                flask_proc = multiprocessing.Process(target=run_flask)
                flask_proc.start()
                logging.info("Waiting for Flask to become ready...")
                wait_for_flask_ready(flask_proc=flask_proc, timeout=wait_timeout)

            if args.no_streamlit or streamlit_proc is None:
                continue

            if streamlit_proc.poll() is not None:
                logging.warning("Streamlit process died. Restarting...")
                streamlit_proc = run_streamlit()

    except KeyboardInterrupt:
        logging.info("Interrupt received, shutting down...")

        try:
            requests.post(f"http://{flask_host()}:{flask_port()}/shutdown", timeout=0.5)
            logging.info("Flask shutdown signal sent.")
        except Exception as e:
            logging.warning(f"Could not shutdown Flask gracefully: {e}")

        if flask_proc and flask_proc.is_alive():
            logging.info("Terminating Flask process...")
            flask_proc.terminate()
            flask_proc.join(timeout=5)
            if flask_proc.is_alive():
                logging.warning("Flask did not terminate, killing...")
                flask_proc.kill()

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
        if flask_proc and flask_proc.is_alive():
            flask_proc.terminate()
        if streamlit_proc and streamlit_proc.poll() is None:
            streamlit_proc.terminate()


if __name__ == "__main__":
    main()
