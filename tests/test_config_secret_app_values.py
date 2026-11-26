import json

import pytest

from config.schemas import CONFIG_SCHEMA
from eve_online_industry_tracker.config.config_manager import ConfigManager


def _write(tmp_path, secret):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"app": {"redirect_uri": "http://localhost:8080/callback"}}), encoding="utf-8")
    sec = tmp_path / "secret.json"
    # utf-8-sig: a BOM from a Windows editor must not break the read
    sec.write_text(json.dumps(secret), encoding="utf-8-sig")
    return str(cfg), str(sec)


def test_client_id_and_user_agent_come_from_secret(tmp_path):
    cfg, sec = _write(tmp_path, {"client_id": "abc", "client_secret": "s", "user_agent": "ua (me@x)",
                                 "characters": [{"character_name": "A"}]})
    m = ConfigManager(cfg, CONFIG_SCHEMA, sec)
    assert m.get("oauth")["client_id"] == "abc"
    assert m.get("app")["user_agent"] == "ua (me@x)"
    assert m.get("app")["redirect_uri"] == "http://localhost:8080/callback"
    assert "client_id" not in m.all()


def test_missing_client_id_points_at_secret(tmp_path):
    cfg, sec = _write(tmp_path, {"client_secret": "s", "characters": [{"character_name": "A"}]})
    with pytest.raises(RuntimeError, match="secret.json"):
        ConfigManager(cfg, CONFIG_SCHEMA, sec)


def test_template_placeholder_is_refused(tmp_path):
    cfg, sec = _write(tmp_path, {"client_id": "YOUR_CLIENT_ID", "client_secret": "s", "characters": []})
    with pytest.raises(RuntimeError, match="placeholder"):
        ConfigManager(cfg, CONFIG_SCHEMA, sec)
