import logging
from pathlib import Path

from config.schemas import CONFIG_SCHEMA
from config.paths import app_config_path, app_secret_path
from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager
from classes.corporation_manager import CorporationManager


def _resolve_sqlite_uri(db_uri: str, *, config_path: Path) -> str:
    """Resolve relative SQLite DB URIs to an absolute path.

    The project config uses URIs like `sqlite:///database/eve_app.db`. If the
    process is started from a different working directory, SQLite will silently
    create/use a different DB file.

    We resolve relative paths against the repository root (inferred from the
    config file location) so both Flask and Streamlit always point at the same
    file.
    """

    if not isinstance(db_uri, str):
        return db_uri

    uri = db_uri.strip()
    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        return db_uri

    raw_path = uri[len(prefix) :]
    if not raw_path:
        return db_uri

    db_path = Path(raw_path)
    if db_path.is_absolute():
        return db_uri

    # config/config.json -> repo root is parent of the `config/` folder.
    try:
        repo_root = config_path.resolve().parent.parent
    except Exception:
        return db_uri

    abs_path = (repo_root / db_path).resolve()
    return f"{prefix}{abs_path.as_posix()}"

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
        raw_uri = cfg["app"]["database_oauth_uri"]
        uri = _resolve_sqlite_uri(str(raw_uri), config_path=cfgManager.base_path)
        logging.debug(f"Database URI for OAuth: {uri}")
        db_oauth = DatabaseManager(uri, cfg["app"]["language"])
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
        raw_uri = cfg["app"]["database_app_uri"]
        uri = _resolve_sqlite_uri(str(raw_uri), config_path=cfgManager.base_path)
        logging.debug(f"Database URI for App: {uri}")
        db_app = DatabaseManager(uri, cfg["app"]["language"])
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
        raw_uri = cfg["app"]["database_sde_uri"]
        uri = _resolve_sqlite_uri(str(raw_uri), config_path=cfgManager.base_path)
        logging.debug(f"Database URI for SDE: {uri}")
        db_sde = DatabaseManager(uri, cfg["app"]["language"])
        return db_sde
    except Exception as e:
        logging.error(f"Failed to initialize SDE database: {e}")
        raise e

def init_db_managers(cfgManager: ConfigManager, refresh_metadata: bool = False) -> tuple[DatabaseManager, DatabaseManager, DatabaseManager]:
    """
    Initialize Databases and Schemas
    """
    from classes.database_models import BaseOauth, BaseApp
    from classes.schema_migrations import ensure_app_schema

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
        logging.error(f"Database and schema initializations failed. {e}", exc_info=True)
        raise e

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
