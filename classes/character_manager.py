
import json
import time
from typing import Optional, List, Dict, Any
from sqlalchemy import text

from classes.config_manager import ConfigManagerSingleton
from classes.database_manager import DatabaseManager
from classes.character import Character

# ----------------------------
# Characters Manager
# ----------------------------
class CharacterManager():
    def __init__(self):
        self.cfg = ConfigManagerSingleton()
        self.db = DatabaseManager(self.cfg.get("app").get("db_characters"))
        self._initialize_cache_table()
        self._initialize_characters_table()
    
    # ----------------------------
    # ESI Cache
    # ----------------------------
    def _initialize_cache_table(self) -> None:
        """Maak de cache-tabel voor ESI responses als die nog niet bestaat."""
        with self.db.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS esi_cache (
                    endpoint TEXT PRIMARY KEY,
                    etag TEXT,
                    data TEXT,
                    last_updated REAL
                )
            """))
    
    def get_etag(self, endpoint: str) -> Optional[str]:
        """Geef de laatste ETag terug voor een endpoint (of None)."""
        with self.db.engine.begin() as conn:
            row = conn.execute(
                text("SELECT etag FROM esi_cache WHERE endpoint = :ep"),
                {"ep": endpoint}
            ).fetchone()
            return row[0] if row else None
    
    def get_cached_response(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Geef de cached response terug voor een endpoint (of None)."""
        with self.db.engine.begin() as conn:
            row = conn.execute(
                text("SELECT data FROM esi_cache WHERE endpoint = :ep"),
                {"ep": endpoint}
            ).fetchone()
            if row:
                try:
                    return json.loads(row[0])
                except json.JSONDecodeError:
                    return None
        return None

    def save_cache(self, endpoint: str, etag: str, data: Dict[str, Any]) -> None:
        """Sla response + ETag op in de cache."""
        with self.db.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO esi_cache (endpoint, etag, data, last_updated)
                VALUES (:ep, :etag, :data, :ts)
                ON CONFLICT(endpoint) DO UPDATE SET
                    etag = excluded.etag,
                    data = excluded.data,
                    last_updated = excluded.last_updated
            """), {
                "ep": endpoint,
                "etag": etag,
                "data": json.dumps(data),
                "ts": time.time()
            })

    # ----------------------------
    # Manage Authenticated Characters
    # ----------------------------
    def _initialize_characters_table(self) -> None:
        """Maak alle nodige tabellen aan als ze nog niet bestaan."""
        with self.db.engine.begin() as conn:
            # Characters tabel
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS characters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    character_name TEXT UNIQUE NOT NULL,
                    character_id INTEGER,
                    refresh_token TEXT NOT NULL,
                    scopes TEXT,
                    is_main INTEGER DEFAULT 0
                )
            """))

    def add_or_update_character(
        self,
        name: str,
        char_id: int,
        refresh_token: str,
        scopes: List[str],
        is_main: bool = False
    ) -> None:
        scopes_json = json.dumps(scopes)
        with self.db.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO characters (character_name, character_id, refresh_token, scopes, is_main)
                VALUES (:name, :char_id, :refresh_token, :scopes, :is_main)
                ON CONFLICT(character_name) DO UPDATE SET
                    character_id = excluded.character_id,
                    refresh_token = excluded.refresh_token,
                    scopes = excluded.scopes,
                    is_main = excluded.is_main
            """), {
                "name": name,
                "char_id": char_id,
                "refresh_token": refresh_token,
                "scopes": scopes_json,
                "is_main": 1 if is_main else 0
            })

            # Zorg dat er maar 1 main character is
            if is_main:
                conn.execute(
                    text("UPDATE characters SET is_main=0 WHERE character_name != :name"),
                    {"name": name}
                )

    def get_character(self, name: str) -> Optional[Character]:
        with self.db.engine.begin() as conn:
            row = conn.execute(
                text("SELECT * FROM characters WHERE character_name = :name"),
                {"name": name}
            ).fetchone()
            if row:
                mapping = row._mapping
                return Character(
                    character_name=mapping["character_name"],
                    is_main=bool(mapping["is_main"])
                )
        return None

    def get_main_character(self) -> Optional[Character]:
        with self.db.engine.begin() as conn:
            row = conn.execute(
                text("SELECT * FROM characters WHERE is_main = 1")
            ).fetchone()
            if row:
                mapping = row._mapping
                return Character(
                    character_name=mapping["character_name"],
                    is_main=bool(mapping["is_main"])
                )
        return None

    def list_characters(self) -> List[Character]:
        """Geeft alle characters terug."""
        with self.db.engine.begin() as conn:
            rows = conn.execute(text("SELECT * FROM characters")).fetchall()
            chars: List[Character] = []
            for row in rows:
                mapping = row._mapping
                chars.append(Character(
                    character_name=mapping["character_name"],
                    is_main=bool(mapping["is_main"])
                ))
            return chars
    
    def update_character_refresh_token(self, character_name: str, new_refresh_token: str) -> None:
        """Update alleen de refresh token van een character."""
        with self.db.engine.begin() as conn:
            conn.execute(
                text("UPDATE characters SET refresh_token = :token WHERE character_name = :name"),
                {"token": new_refresh_token, "name": character_name}
            )
