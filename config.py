import json
from pathlib import Path
from typing import Any, Dict

class ConfigManager:
    def __init__(self, config_path: str = "config/config.json"):
        self.config_file = Path(config_path)
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if self.config_file.exists():
            return json.loads(self.config_file.read_text())
        else:
            raise FileNotFoundError(f"{self.config_file} not found")

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._config[key] = value
        self.save()

    def save(self) -> None:
        self.config_file.write_text(json.dumps(self._config, indent=4))

    def all(self) -> Dict[str, Any]:
        return self._config