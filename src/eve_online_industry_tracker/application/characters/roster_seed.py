"""One-time copy of the secret.json character list into the OAuth DB roster."""
from __future__ import annotations

from sqlalchemy import text

from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager

# Names returned by the last seed run; Task 3's bootstrap reads this.
LAST_SEED_UNCONFIGURED: list[str] = []

_KEY = "characters_seeded"


def seed_characters_once(db_oauth: DatabaseManager, cfg_characters: list[dict]) -> list[str]:
    """Add/flag roster rows from cfg once. Returns activated names missing from cfg."""
    chars = [dict(c) for c in cfg_characters]
    # Old fallbacks: no main / no director configured -> the first character.
    if chars and not any(c.get("is_main") for c in chars):
        chars[0]["is_main"] = True
    if chars and not any(c.get("is_corp_director") for c in chars):
        chars[0]["is_corp_director"] = True

    with db_oauth.engine.begin() as conn:
        if conn.execute(text("SELECT 1 FROM oauth_meta WHERE key = :k"), {"k": _KEY}).first():
            return []
        rows = conn.execute(text("SELECT id, character_name, refresh_token FROM oauth_characters")).fetchall()
        by_name = {r[1].casefold(): r for r in rows}
        if any(c.get("is_main") for c in chars):  # a stored main outside cfg must not stay a second main
            conn.execute(text("UPDATE oauth_characters SET is_main = 0"))
        for c in chars:
            params = {
                "n": c["character_name"],
                "m": bool(c.get("is_main", False)),
                "d": bool(c.get("is_corp_director", False)),
                "i": bool(c.get("industry_enabled", True)),
            }
            if c["character_name"].casefold() in by_name:
                conn.execute(text(
                    "UPDATE oauth_characters SET is_main=:m, is_corp_director=:d, industry_enabled=:i "
                    "WHERE id=:id"), {**params, "id": by_name[c["character_name"].casefold()][0]})
            else:
                conn.execute(text(
                    "INSERT INTO oauth_characters (character_name, is_main, is_corp_director, industry_enabled) "
                    "VALUES (:n, :m, :d, :i)"), params)
        conn.execute(text("INSERT INTO oauth_meta (key, value) VALUES (:k, '1')"), {"k": _KEY})
    cfg_names = {c["character_name"].casefold() for c in chars}
    return [r[1] for r in rows if r[2] and r[1].casefold() not in cfg_names]
