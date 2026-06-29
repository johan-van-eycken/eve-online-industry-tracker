import logging

from config.schemas import CONFIG_SCHEMA
from config.paths import app_config_path, app_secret_path
from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.application.characters.character_manager import CharacterManager
from eve_online_industry_tracker.application.corporations.corporation_manager import CorporationManager


def _user_friendly_error(e: Exception) -> str | None:
    """Return a clean, actionable message for known error types, or None for unexpected ones."""
    msg = str(e)
    etype = type(e).__name__

    if "CERTIFICATE_VERIFY_FAILED" in msg or "SSLError" in etype:
        return (
            "SSL certificate verification failed.\n"
            "  Likely cause: corporate proxy (e.g. Zscaler) intercepting HTTPS.\n"
            "  Fix: pip install truststore  (uses macOS Keychain certificates)"
        )

    if "no such table" in msg.lower():
        table = msg.split("no such table: ")[-1].strip().split()[0] if "no such table: " in msg else "unknown"
        sde_tables = {"types", "groups", "categories", "blueprints", "races", "factions",
                      "mapregions", "mapconstellations", "mapsolarsystems", "npcstations"}
        if any(t in table.lower() for t in sde_tables):
            return (
                f'SDE database is missing table "{table}".\n'
                "  The EVE Static Data Export has not been imported yet.\n"
                "  Fix: python3 scripts/import_sde.py --download --import --force"
            )
        return (
            f'App database is missing table "{table}".\n'
            "  This can happen when moving to a new machine without copying the database files.\n"
            "  Fix: delete database/eve_app.db and restart — the app will rebuild it automatically.\n"
            "  Note: your OAuth tokens are stored separately in database/eve_oauth.db and are unaffected."
        )

    if "no such column" in msg.lower():
        col = msg.split("no such column: ")[-1].strip().split()[0] if "no such column: " in msg else "unknown"
        return (
            f'Database column "{col}" is missing.\n'
            "  The app database is from an older version and is missing a new column.\n"
            "  Fix: delete database/eve_app.db and restart — the app will rebuild it automatically.\n"
            "  Note: your OAuth tokens are stored separately in database/eve_oauth.db and are unaffected."
        )

    if "UNIQUE constraint failed" in msg:
        field = msg.split("UNIQUE constraint failed: ")[-1].strip().split("\n")[0] if "UNIQUE constraint failed: " in msg else ""
        detail = f" ({field})" if field else ""
        return (
            f"Duplicate record{detail} — a character may already be registered.\n"
            "  This is usually safe to ignore on startup."
        )

    if "NOT NULL constraint failed" in msg:
        field = msg.split("NOT NULL constraint failed: ")[-1].strip().split("\n")[0] if "NOT NULL constraint failed: " in msg else ""
        detail = f" ({field})" if field else ""
        return (
            f"Database error: required field is empty{detail}.\n"
            "  ESI returned incomplete data for this character. Try re-running the app."
        )

    if "Invalid character configuration" in msg:
        return (
            "Invalid character configuration in config/secret.json.\n"
            "  Each character must be a dict, for example:\n"
            '    {"character_name": "Your Name", "is_main": true, "is_corp_director": false}'
        )

    if "No characters found in config" in msg:
        return (
            f"No characters configured in {app_secret_path()}.\n"
            "  Add at least one character to the 'characters' list."
        )

    return None


def load_config() -> ConfigManager:
    """
    Load Configurations
    """
    try:
        cfgManager = ConfigManager(
            base_path=app_config_path(),
            secret_path=app_secret_path(),
            schema=CONFIG_SCHEMA,
        )
        cfg = cfgManager.all()
        if len(cfg["characters"]) == 0:
            raise ValueError(
                "No characters found in config. Add a 'characters' list to your secret config "
                f"({app_secret_path()}) to avoid committing character names."
            )
        return cfgManager
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        raise e
    
def init_db_oauth(cfgManager: ConfigManager) -> DatabaseManager:
    """
    Initialize OAuth Database
    """
    try:
        cfg = cfgManager.all()
        logging.debug(f"Database URI for OAuth: {cfg['app']['database_oauth_uri']}")
        db_oauth = DatabaseManager(cfg["app"]["database_oauth_uri"], cfg["app"]["language"])
        return db_oauth
    except Exception as e:
        logging.error(f"Failed to initialize OAuth database: {e}")
        raise e

def init_db_app(cfgManager: ConfigManager) -> DatabaseManager:
    """
    Initialize App Database
    """
    try:
        cfg = cfgManager.all()
        logging.debug(f"Database URI for App: {cfg['app']['database_app_uri']}")
        db_app = DatabaseManager(cfg["app"]["database_app_uri"], cfg["app"]["language"])
        return db_app
    except Exception as e:
        logging.error(f"Failed to initialize App database: {e}")
        raise e

def init_db_sde(cfgManager: ConfigManager) -> DatabaseManager:
    """
    Initialize SDE Database
    """
    try:
        cfg = cfgManager.all()
        logging.debug(f"Database URI for SDE: {cfg['app']['database_sde_uri']}")
        db_sde = DatabaseManager(cfg["app"]["database_sde_uri"], cfg["app"]["language"])
        return db_sde
    except Exception as e:
        logging.error(f"Failed to initialize SDE database: {e}")
        raise e

def init_db_managers(cfgManager: ConfigManager, refresh_metadata: bool = False) -> tuple[DatabaseManager, DatabaseManager, DatabaseManager]:
    """
    Initialize Databases and Schemas
    """
    from eve_online_industry_tracker.infrastructure.models import BaseOauth, BaseApp
    from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_app_schema

    try:
        cfg = cfgManager.all()

        logging.debug(f"Database URI for OAuth: {cfg['app']['database_oauth_uri']}")
        db_oauth = init_db_oauth(cfgManager)
        if refresh_metadata:
            BaseOauth.metadata.create_all(bind=db_oauth.engine)

        logging.debug(f"Database URI for App: {cfg['app']['database_app_uri']}")
        db_app = init_db_app(cfgManager)
        if refresh_metadata:
            BaseApp.metadata.create_all(bind=db_app.engine)

        # Forward-migrate app DB schema (SQLite) for new columns.
        ensure_app_schema(db_app)

        logging.debug(f"Database URI for SDE: {cfg['app']['database_sde_uri']}")
        db_sde = init_db_sde(cfgManager)

        return db_oauth, db_app, db_sde
    except Exception as e:
        friendly = _user_friendly_error(e)
        if friendly:
            logging.error("Database initialization failed:\n  %s", friendly.replace("\n", "\n  "))
        else:
            logging.error("Database initialization failed: %s", e, exc_info=True)
        raise

def init_char_manager(cfgManager: ConfigManager, db_oauth: DatabaseManager, db_app: DatabaseManager, db_sde: DatabaseManager) -> CharacterManager:
    """
    Initialize Character Manager
    """
    try:
        char_manager = CharacterManager(cfgManager, db_oauth, db_app, db_sde)
        return char_manager
    except ValueError as e:
        logging.error(f"Error encountered: {e}")
        raise e
    except Exception as e:
        logging.error(f"Failed to initialize characters: {e}")
        raise e
    
def init_corp_manager(cfgManager: ConfigManager, db_oauth: DatabaseManager, db_app: DatabaseManager, db_sde: DatabaseManager, char_manager: CharacterManager) -> CorporationManager:
    """
    Initialize Corporation Manager
    """
    try:
        corp_manager = CorporationManager(cfgManager, db_oauth, db_app, db_sde, char_manager)
        return corp_manager
    except ValueError as e:
        logging.error(f"Error encountered: {e}")
        raise e
    except Exception as e:
        logging.error(f"Failed to initialize corporations: {e}")
        raise e
