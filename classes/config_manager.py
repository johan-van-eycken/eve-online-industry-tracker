import json
import logging
from pathlib import Path
from typing import Any, Dict

class ConfigManager:
    """
    Loads and validates configuration from base and secret JSON files.
    Applies defaults for missing optional keys and enforces required keys.
    """
    # Define required sections/keys and optional defaults
    REQUIRED_KEYS = {
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
            "verify_url": "https://login.eveonline.com/oauth/verify"
        },
        "oauth": {
            "client_id": "84f5c62c020c46559d2b8615ea1eb146"
        },
        "characters": [],      # Must be a list, no defaults
        "client_secret": None  # must be filled in secret.json
    }

    def __init__(
        self,
        base_path: str = "config/config.json",
        secret_path: str = "config/secret.json"
    ):
        self._config: Dict[str, Any] = {}
        self._load_config(base_path)
        self._load_secret(secret_path)
        self._validate_config()

    # ----------------------------
    # Internal loaders
    # ----------------------------
    def _load_config(self, path: str) -> None:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"{p} not found. Please create it.")
        self._config.update(json.loads(p.read_text()))

    def _load_secret(self, path: str) -> None:
        p = Path(path)
        if not p.exists():
            # Auto-create secret.json with placeholder
            p.parent.mkdir(parents=True, exist_ok=True)
            default_secret = {"client_secret": "YOUR_SECRET_CLIENT_ID"}
            p.write_text(json.dumps(default_secret, indent=4))
            raise RuntimeError(f"{p} created with placeholder. Please fill in your EVE client_secret.")

        secret_data = json.loads(p.read_text())
        if secret_data.get("client_secret") == "YOUR_SECRET_CLIENT_ID":
            raise RuntimeError(f"{p} contains placeholder client_secret. Please update it with your real secret.")

        self._config.update(secret_data)

    def _set_config_value(self, section: str, key: str, value: Any) -> None:
        if section not in self._config:
            self._config[section] = {}
        self._config[section][key] = value

    # ----------------------------
    # Validation and defaults
    # ----------------------------
    def _validate_config(self) -> None:
        """
        Ensure all required keys are present, apply defaults for optional keys.
        """
        for section, keys in self.REQUIRED_KEYS.items():
            if section not in self._config:
                if keys is None:
                    raise RuntimeError(f"Required config section '{section}' missing.")
                if isinstance(keys, dict):
                    self._config[section] = {}
                elif isinstance(keys, list):
                    self._config[section] = []

            if keys is None:
                # Single value required (like client_secret)
                if not self._config.get(section):
                    raise RuntimeError(f"Required config '{section}' missing and has no default.")
            elif isinstance(keys, dict):
                for k, default in keys.items():
                    if k not in self._config[section]:
                        if default is None:
                            raise RuntimeError(f"Required config '{section}.{k}' missing and has no default.")
                        logging.warning(f"Config '{section}.{k}' missing. Using default: {default}")
                        self._set_config_value(section, k, default)
            elif isinstance(keys, list):
                if not isinstance(self._config[section], list):
                    raise RuntimeError(f"Required config '{section}' must be a list.")

    # ----------------------------
    # Public interface
    # ----------------------------
    def get(self, key: str, default: Any = None) -> Any:
        """Return a top-level key, or default."""
        return self._config.get(key, default)

    def all(self) -> Dict[str, Any]:
        """Return full config dictionary."""
        return self._config
