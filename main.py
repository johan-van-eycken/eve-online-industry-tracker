import logging
import threading
import subprocess
import requests
import sys
import os
import time
from classes.config_manager import ConfigManager
from config.schemas import CONFIG_SCHEMA
from classes.database_manager import DatabaseManager
from classes.database_models import BaseOauth, BaseApp
from classes.character_manager import CharacterManager
from classes.corporation_manager import CorporationManager

FLASK_HOST = 'localhost'
FLASK_PORT = 5000
STREAMLIT_HOST = 'localhost'
STREAMLIT_PORT = 8501

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
        requests.post(f"http://{FLASK_HOST}:{FLASK_PORT}/shutdown")
    except Exception as e:
        logging.warning(f"Could not shutdown Flask gracefully: {e}")
    # Shutdown Streamlit
    if streamlit_proc:
        streamlit_proc.terminate()
        streamlit_proc.wait()
    # Wait for Flask thread to finish
    flask_thread.join()
    logging.info("Shutdown complete.")

def initialize_eve_oauth_schema(database_manager: DatabaseManager):
    """Initialize the database schema for eve_oauth.db."""
    logging.debug(f"Initializing database schema for {database_manager.get_db_name()}...")
    try:
        BaseOauth.metadata.create_all(bind=database_manager.engine)
    except Exception as e:
        logging.error(f"Failed to initialize schema: {e}")
        raise e
    logging.debug(f"Database schema for `{database_manager.get_db_name()}` initialized successfully.")

def initialize_eve_app_schema(database_manager: DatabaseManager):
    """Initialize the database schema for eve_app.db."""
    logging.debug(f"Initializing database schema for {database_manager.get_db_name()}...")
    try:
        BaseApp.metadata.create_all(bind=database_manager.engine)
    except Exception as e:
        logging.error(f"Failed to initialize schema: {e}")
        raise e
    logging.debug(f"Database schema for `{database_manager.get_db_name()}` initialized successfully.")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load Configurations
    logging.info("Loading config...")
    try:
        cfgManager = ConfigManager(base_path="config/config.json", secret_path="config/secret.json", schema=CONFIG_SCHEMA)
        cfg = cfgManager.all()
        cfg_language = cfg["app"]["language"]
        cfg_characters = cfg["characters"]
        if len(cfg_characters) == 0:
            raise ValueError("No characters found in config!")
        cfg_oauth_db_uri = cfg["app"]["database_oauth_uri"]
        cfg_app_db_uri = cfg["app"]["database_app_uri"]
        cfg_sde_db_uri = cfg["app"]["database_sde_uri"]

    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        return
    logging.debug("Config loaded successfully.")

    # Initialize Databases and Schemas
    logging.info("Initializing databases...")
    try:
        logging.debug(f"Database URI for OAuth: {cfg_oauth_db_uri}")
        db_oauth = DatabaseManager(cfg_oauth_db_uri, cfg_language)
        initialize_eve_oauth_schema(db_oauth)

        logging.debug(f"Database URI for App: {cfg_app_db_uri}")
        db_app = DatabaseManager(cfg_app_db_uri, cfg_language)
        initialize_eve_app_schema(db_app)

        logging.debug(f"Database URI for SDE: {cfg_sde_db_uri}")
        db_sde = DatabaseManager(cfg_sde_db_uri, cfg_language)
    except Exception as e:
        logging.error(f"Database and schema initializations failed. {e}", exc_info=True)
        return

    # Initialize Character Manager
    logging.info("Initializing characters...")
    try:
        char_manager_all = CharacterManager(cfgManager, db_oauth, db_app, db_sde)
        char_manager_all.refresh_all()
    except ValueError as e:
        logging.error(f"Error encountered: {e}")
        return
    except Exception as e:
        logging.error(f"Failed to initialize characters: {e}")
        return
    
    # Initialize Corporation Manager
    logging.info("Initializing corporations...")
    try:
        corp_manager = CorporationManager(cfgManager, db_oauth, db_app, db_sde, char_manager_all)
    except ValueError as e:
        logging.error(f"Error encounted: {e}")
        return
    except Exception as e:
        logging.error(f"Failed to initialize corporations: {e}")
        return
    
    chars_initialized = len(char_manager_all.character_list)
    corps_initialized = len(corp_manager.corporation_ids)
    logging.info(f"All done. Characters: {chars_initialized}, Corporations: {corps_initialized}")

    # Start Flask and Streamlit in separate threads
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    streamlit_proc = run_streamlit()

    try:
        while flask_thread.is_alive() and streamlit_proc.poll() is None:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Interrupt received, shutting down...")
        try:
            requests.post(f"http://{FLASK_HOST}:{FLASK_PORT}/shutdown")
        except Exception as e:
            print(f"Could not shutdown Flask gracefully: {e}")
        # Gracefully shutdown Streamlit
        if streamlit_proc.poll() is None:
            streamlit_proc.terminate()
            try:
                streamlit_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                streamlit_proc.kill()
        flask_thread.join(timeout=5)
        os._exit(0)
        sys.exit(0)

if __name__ == "__main__":
    main()