# config_manager.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigManager:
    """
    Loads and validates configuration from a JSON file, with schema-based validation.
    """

    def __init__(
        self,
        base_path: str,
        schema: Dict[str, Any],
        secret_path: Optional[str] = None,
    ):
        self._config: Dict[str, Any] = {}
        self.schema = schema
        self.base_path = Path(base_path)
        self.secret_path = Path(secret_path) if secret_path else None

        self._load_config(self.base_path)

        if self.secret_path:
            self._load_secret(self.secret_path)

        self._validate_config()

    # ----------------------------
    # Internal loaders
    # ----------------------------
    def _load_config(self, path: Path) -> None:
        if not path.exists():
            raise FileNotFoundError(f"{path} not found. Please create it.")
        self._config.update(json.loads(path.read_text()))

    def _load_secret(self, path: Path) -> None:
        if not path.exists():
            # Auto-create secret.json with placeholder
            path.parent.mkdir(parents=True, exist_ok=True)
            default_secret = {"client_secret": "YOUR_SECRET_CLIENT_ID"}
            path.write_text(json.dumps(default_secret, indent=4))
            raise RuntimeError(
                f"{path} created with placeholder. Please fill in your EVE client_secret."
            )

        secret_data = json.loads(path.read_text())
        if secret_data.get("client_secret") == "YOUR_SECRET_CLIENT_ID":
            raise RuntimeError(
                f"{path} contains placeholder client_secret. Please update it."
            )

        self._config.update(secret_data)

    def _set_config_value(self, section: str, key: str, value: Any) -> None:
        if section not in self._config:
            self._config[section] = {}
        self._config[section][key] = value

    # ----------------------------
    # Validation
    # ----------------------------
    def _validate_config(self) -> None:
        for section, keys in self.schema.items():
            if section not in self._config:
                if keys is None:
                    raise RuntimeError(f"Required config section '{section}' missing.")
                if isinstance(keys, dict):
                    self._config[section] = {}
                elif isinstance(keys, list):
                    self._config[section] = []

            if keys is None:
                if not self._config.get(section):
                    raise RuntimeError(
                        f"Required config '{section}' missing and has no default."
                    )
            elif isinstance(keys, dict):
                for k, default in keys.items():
                    if k not in self._config[section]:
                        if default is None:
                            raise RuntimeError(
                                f"Required config '{section}.{k}' missing and has no default."
                            )
                        logging.warning(
                            f"Config '{section}.{k}' missing. Using default: {default}"
                        )
                        self._set_config_value(section, k, default)
            elif isinstance(keys, list):
                if not isinstance(self._config[section], list):
                    raise RuntimeError(
                        f"Required config '{section}' must be a list."
                    )

    # ----------------------------
    # Public interface
    # ----------------------------
    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def all(self) -> Dict[str, Any]:
        return self._config
