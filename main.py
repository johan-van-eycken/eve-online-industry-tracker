import logging
import multiprocessing
import subprocess
import time

from utils.flask_api import FLASK_HOST, FLASK_PORT, api_get


def run_flask():
    """Start the Flask app"""
    from flask_app.flask_app import app

    app.run(host=FLASK_HOST, port=FLASK_PORT)


def run_streamlit():
    """Start the Streamlit app in a subprocess"""
    return subprocess.Popen(["streamlit", "run", "streamlit_app.py"])


def wait_for_flask_ready(timeout=120):
    """Wait for Flask to become ready"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            flask_health = api_get("/health")
            if flask_health and flask_health.get("status") == "OK":
                logging.info("Flask is ready!")
                return True
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("Flask did not become ready in time.")


def main():
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    flask_proc = None
    streamlit_proc = None

    try:
        # Start Flask first
        logging.info("Starting Flask app...")
        flask_proc = multiprocessing.Process(target=run_flask)
        flask_proc.start()

        logging.info("Waiting for Flask to become ready...")
        wait_for_flask_ready(timeout=120)

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
                wait_for_flask_ready(timeout=120)

            # Check if Streamlit died (optional)
            if streamlit_proc.poll() is not None:
                logging.warning("Streamlit process died. Restarting...")
                streamlit_proc = run_streamlit()

    except KeyboardInterrupt:
        logging.info("Interrupt received, shutting down...")

        # Shutdown Flask gracefully
        try:
            api_get("/shutdown")
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