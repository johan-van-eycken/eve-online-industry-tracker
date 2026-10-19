"""Schema definitions for config files.

This module is packaged under src/ so imports like:

    from config.schemas import CONFIG_SCHEMA

work after `pip install -e .`. It is the only copy (the repo-root
config/schemas.py was a stale duplicate and is removed).
"""

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
        "base": "https://esi.evetech.net",
        "auth_url": "https://login.eveonline.com/v2/oauth/authorize/",
        "token_url": "https://login.eveonline.com/v2/oauth/token",
        "verify_url": "https://login.eveonline.com/oauth/verify",
        "headers": {
            "Accept": "application/json",  # Required
            "Accept-Language": "en",  # Default language
            # Informational: ESI requests always send esi_versions.ESI_COMPATIBILITY_DATE.
            "X-Compatibility-Date": "2025-08-26",
            "X-Tenant": "tranquility",  # Tenant identifier
        },
    },
    "oauth": {"client_id": None},  # no default allowed
    "characters": [],  # must be a list
    "client_secret": None,  # required from secret.json
    "defaults": {
        "scopes": [],
        "scopes_corp_director": [],
        "market_pricing": {
            "material_price_source_default": "Jita Sell",
            "product_price_source_default": "Jita Sell",
            "orderbook_smoothing": "median_best_n",
            # orderbook_depth and the material price cache TTL are admin settings
            # (market_defaults/orderbook_depth, cache_ttl/material_price_cache_ttl_seconds);
            # the sales tax and broker fee come from the character (industry/*).
        },
    },
}

IMPORT_SDE_SCHEMA = {
    "APP_VERSION": None,
    "SDE_VERSION": None,
    "SDE_URL": None,
    "DEFAULT_DB_URI": "sqlite:///database/eve_sde.db",
    "DEFAULT_TMP_DIR": "database/data/tmp_sde",
    "TABLES_TO_IMPORT": [],
}

SDE_VERSION_SCHEMA = {
    "table": "sde_version",
    "columns": [
        {"name": "id", "type": "INTEGER PRIMARY KEY AUTOINCREMENT"},
        {"name": "build_number", "type": "INTEGER NOT NULL UNIQUE"},
        {"name": "release_date", "type": "TEXT NOT NULL"},
        {"name": "imported_at", "type": "TEXT NOT NULL"},
        {"name": "is_current", "type": "INTEGER DEFAULT 0"},
    ],
}
