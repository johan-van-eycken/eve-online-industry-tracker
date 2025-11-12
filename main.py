import logging
import multiprocessing
import subprocess
import os
import time

from utils.flask_api import FLASK_HOST, FLASK_PORT, api_get


# Start the backend Flask app in a separate thread
def run_flask():
    from flask_app.flask_app import app
    app.run(host=FLASK_HOST, port=FLASK_PORT)

# Start the Streamlit app in a separate thread
def run_streamlit():
    # Use subprocess so we can terminate it later
    return subprocess.Popen(["streamlit", "run", "streamlit_app.py"])

def graceful_shutdown(flask_thread, streamlit_proc):
    logging.info("Graceful shutdown initiated...")
    # Shutdown Flask
    try:
        api_get("/shutdown")
    except Exception as e:
        logging.warning(f"Could not shutdown Flask gracefully: {e}")
    
    # Shutdown Streamlit
    if streamlit_proc:
        streamlit_proc.terminate()
        streamlit_proc.wait()
    # Wait for Flask thread to finish
    flask_thread.join()
    logging.info("Shutdown complete.")

def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    streamlit_proc = run_streamlit()

    flask_proc = None
    try:
        while True:
            if flask_proc is None or not flask_proc.is_alive():
                logging.info("Starting Flask app...")
                flask_proc = multiprocessing.Process(target=run_flask)
                flask_proc.start()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Interrupt received, shutting down...")
        # Shutdown Flask
        try:
            api_get("/shutdown")
        except Exception as e:
            logging.warning(f"Could not shutdown Flask gracefully: {e}")
        # Shutdown Streamlit
        if streamlit_proc.poll() is None:
            streamlit_proc.terminate()
            try:
                streamlit_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                streamlit_proc.kill()
        if flask_proc.is_alive():
            flask_proc.terminate()
            flask_proc.join(timeout=5)
        os._exit(0)

if __name__ == "__main__":
    main()