import json
from pathlib import Path
from typing import Any, Dict

class ConfigManager:
    def __init__(self, base_path: str = "config/config.json", secret_path: str = "config/secret.json"):
        self._config: Dict[str, Any] = {}
        self._load_config(base_path)
        self._load_secret(secret_path)

    def _load_config(self, path: str) -> None:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"{p} not found. Please create it.")
        self._config.update(json.loads(p.read_text()))

    def _load_secret(self, path: str) -> None:
        p = Path(path)
        if not p.exists():
            # Maak automatisch secret.json aan met placeholder
            p.parent.mkdir(parents=True, exist_ok=True)
            default_secret = {"client_secret": "YOUR_SECRET_CLIENT_ID"}
            p.write_text(json.dumps(default_secret, indent=4))
            raise RuntimeError(f"{p} created with placeholder. Please fill in your EVE client_secret.")

        secret_data = json.loads(p.read_text())
        if secret_data.get("client_secret") == "YOUR_SECRET_CLIENT_ID":
            raise RuntimeError(f"{p} contains placeholder client_secret. Please update it with your real secret.")

        self._config.update(secret_data)

    # Read-only get
    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def all(self) -> Dict[str, Any]:
        return self._config
