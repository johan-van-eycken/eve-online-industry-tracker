import logging

from classes.config_manager import ConfigManager
from config.schemas import CONFIG_SCHEMA
from classes.character_manager import CharacterManager
from classes.database_manager import DatabaseManager
from classes.database_models import Base

def initialize_oauth_schema(database_manager: DatabaseManager):
    """Initialize the database schema for OAuth."""
    logging.debug("Initializing database schema for OAuth...")
    try:
        Base.metadata.create_all(bind=database_manager.engine)
    except Exception as e:
        logging.error(f"Failed to initialize schema: {e}")
        raise e
    logging.debug("Database schema for OAuth initialized successfully.")

def main():
    logging.basicConfig(level=logging.INFO)

    # Load Configurations
    logging.info("Loading config...")
    try:
        cfg = ConfigManager(base_path="config/config.json", secret_path="config/secret.json", schema=CONFIG_SCHEMA)._config
        cfg_language = cfg.get("app").get("language")
        cfg_characters = cfg.get("characters")
        if len(cfg_characters) == 0:
            raise ValueError("No characters found in config!")
        cfg_oauth_db_uri = cfg.get("app").get("database_oauth_uri")
        cfg_app_db_uri = cfg.get("app").get("database_app_uri")
        cfg_sde_db_uri = cfg.get("app").get("database_sde_uri")
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        return
    logging.debug("Config loaded successfully.")

    # Initialize Database Schema
    try:
        logging.debug(f"Database URI for OAuth: {cfg_oauth_db_uri}")
        db_oauth = DatabaseManager(cfg_oauth_db_uri, cfg_language)
        initialize_oauth_schema(db_oauth)
    except Exception as e:
        logging.error(f"Schema initialization failed. {e}")
        return

    # Initialize Character Manager
    logging.info("Initializing characters...")
    try:
        db_app = DatabaseManager(cfg_app_db_uri, cfg_language)
        db_sde = DatabaseManager(cfg_sde_db_uri, cfg_language)
        char_manager = CharacterManager(cfg, db_oauth, db_app, db_sde, cfg_characters)
    except ValueError as e:
        logging.error(f"Error encountered: {e}")
        return
    except Exception as e:
        logging.error(f"Failed to initialize characters: {e}")
        return
    logging.debug("Characters initialized successfully.")
    
    # Refresh wallet balances for all Characters
    char_manager.refresh_wallet_balance()
    
if __name__ == "__main__":
    main()