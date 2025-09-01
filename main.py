import logging
from alembic.config import Config
from alembic import command
from classes.config_manager import ConfigManager
from config.schemas import CONFIG_SCHEMA
from classes.character_manager import CharacterManager
from classes.database_manager import DatabaseManager
from classes.database_models import BaseOauth, BaseApp

def sync_app_database():
    """Sync eve_app.db with all database models in classes/database_models.py"""
    logging.debug("Starting database sync for eve_app.db using Alembic...")
    try:
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head", sql=False)  # Apply migrations
        
    except Exception as e:
        logging.error("Error during database sync for eve_app.db.")
        raise e
    logging.debug("Database migrations applied successfully.")

def initialize_eve_oauth_schema(database_manager: DatabaseManager):
    """Initialize the database schema for eve_oauth.db."""
    logging.debug(f"Initializing database schema for {database_manager.get_db_name()}...")
    try:
        BaseOauth.metadata.create_all(bind=database_manager.engine)
    except Exception as e:
        logging.error(f"Failed to initialize schema: {e}")
        raise e
    logging.debug(f"Database schema for `{database_manager.get_db_name()}` initialized successfully.")

def main():
    logging.basicConfig(level=logging.INFO)

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
        # sync_app_database()
        db_app = DatabaseManager(cfg_app_db_uri, cfg_language)

        logging.debug(f"Database URI for Sde: {cfg_sde_db_uri}")
        db_sde = DatabaseManager(cfg_sde_db_uri, cfg_language)
    except Exception as e:
        logging.error(f"Database and schema initializations failed. {e}", exc_info=True)
        return

    # Initialize Character Manager
    logging.info("Initializing characters...")
    try:
        char_manager = CharacterManager(cfgManager, db_oauth, db_app, db_sde, cfg_characters)
        char_manager.refresh_all()

    except ValueError as e:
        logging.error(f"Error encountered: {e}")
        return
    
    except Exception as e:
        logging.error(f"Failed to initialize characters: {e}")
        return
    
    logging.debug("Characters initialized successfully.")
    
if __name__ == "__main__":
    main()