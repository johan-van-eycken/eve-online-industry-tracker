import json
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List, Dict, Any
from classes.types import CharacterRow

DB_FILE = "database/eve_data.db"

# ----------------------------
# Algemeen Database Manager
# ----------------------------
class DatabaseManager:
    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self.engine = create_engine(f"sqlite:///{self.db_file}")
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Maak alle nodige tabellen aan als ze nog niet bestaan."""
        with self.engine.begin() as conn:
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

    # DataFrame helper functies
    def save_df(self, df: pd.DataFrame, table_name: str) -> None:
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def load_df(self, table_name: str) -> pd.DataFrame:
        return pd.read_sql(f"SELECT * FROM {table_name}", self.engine)


# ----------------------------
# Characters Manager
# ----------------------------
class CharacterManager(DatabaseManager):
    def add_or_update_character(
        self,
        name: str,
        char_id: int,
        refresh_token: str,
        scopes: List[str],
        is_main: bool = False
    ) -> None:
        scopes_json = json.dumps(scopes)
        with self.engine.begin() as conn:
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

    def get_character(self, name: str) -> Optional[CharacterRow]:
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT * FROM characters WHERE character_name = :name"),
                {"name": name}
            ).fetchone()
            if row:
                mapping = row._mapping
                return CharacterRow(
                    character_name=mapping["character_name"],
                    character_id=mapping["character_id"],
                    refresh_token=mapping["refresh_token"],
                    is_main=bool(mapping["is_main"])
                )
        return None

    def get_main_character(self) -> Optional[CharacterRow]:
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT * FROM characters WHERE is_main = 1")
            ).fetchone()
            if row:
                mapping = row._mapping
                return CharacterRow(
                    character_name=mapping["character_name"],
                    character_id=mapping["character_id"],
                    refresh_token=mapping["refresh_token"],
                    is_main=bool(mapping["is_main"])
                )
        return None
    
    def set_main_character(self, name: str) -> None:
        """Set the given character as main, and unset main for all others."""
        with self.engine.begin() as conn:
            if not self.get_character(name):
                raise ValueError(f"Character '{name}' not found in database.")
            conn.execute(
                text("UPDATE characters SET is_main = CASE WHEN character_name = :name THEN 1 ELSE 0 END"),
                {"name": name}
            )

    def list_characters(self) -> List[Dict[str, Any]]:
        """Geeft alle characters terug als dicts (ipv CharacterRow)."""
        with self.engine.begin() as conn:
            rows = conn.execute(text("SELECT * FROM characters")).fetchall()
            result: List[Dict[str, Any]] = []
            for row in rows:
                data = dict(row._mapping)
                data["scopes"] = json.loads(data["scopes"]) if data["scopes"] else []
                data["is_main"] = bool(data["is_main"])
                result.append(data)
            return result
    
    def update_character_refresh_token(self, character_name: str, new_refresh_token: str) -> None:
        """Update alleen de refresh token van een character."""
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE characters SET refresh_token = :token WHERE character_name = :name"),
                {"token": new_refresh_token, "name": character_name}
            )
