# schemas.py
CONFIG_SCHEMA = {
    "app": {
        "user_agent": "EVE Industry Tracker",
        "database_path": "database",
        "database_oauth_uri": "sqlite:///database/eve_oauth.db",
        "database_app_uri": "sqlite:///database/eve_app.db",
        "database_sde_uri": "sqlite:///database/eve_sde.db",
        "language": "en",
    },
    "esi": {
        "base": "https://esi.evetech.net/latest",
        "auth_url": "https://login.eveonline.com/v2/oauth/authorize/",
        "token_url": "https://login.eveonline.com/v2/oauth/token",
        "verify_url": "https://login.eveonline.com/oauth/verify",
        "headers": {
            "Accept": "application/json",  # Required
            "Accept-Language": "en",  # Default language
            "X-Compatibity-Date": "2025-08-26",  # Required compatibility date
            "X-Tenant": "tranquility",  # Tenant identifier
        },
    },
    "oauth": {"client_id": None},  # no default allowed
    "characters": [],  # must be a list
    "client_secret": None,  # required from secret.json
    "defaults": {"scopes": [], "scopes_corp_director": []}
}

IMPORT_SDE_SCHEMA = {
    "APP_VERSION": None,
    "SDE_URL": None,
    "DEFAULT_DB_URI": "sqlite:///database/eve_sde.db",
    "DEFAULT_TMP_DIR": "database/data/tmp_sde",
    "TABLES_TO_IMPORT": [],
}
