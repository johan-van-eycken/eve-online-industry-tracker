import json
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List

from classes.config_manager import ConfigManagerSingleton

# ----------------------------
# Algemeen Database Manager
# ----------------------------
class DatabaseManager:
    def __init__(self, db_file: str):
        self.cfg = ConfigManagerSingleton()
        self.db_file = f"{self.cfg.get('app').get('database_path', 'database')}/{db_file}"
        self.language = self.cfg.get("app").get("language", "en")
        self.engine = create_engine(f"sqlite:///{self.db_file}")

    # Database helper functies
    def get_db_name(self) -> str:
        """Geeft de bestandsnaam van de database terug."""
        return self.db_file.split("/")[-1]
    
    def list_tables(self) -> List[str]:
        """Lijst alle tabellen in de database."""
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
            return [row[0] for row in result.fetchall()]
        
    def drop_table(self, table_name: str) -> None:
        """Verwijder een tabel uit de database."""
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    # DataFrame helper functies
    def save_df(self, df: pd.DataFrame, table_name: str) -> None:
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def load_df(self, table_name: str, language: Optional[str] = None) -> pd.DataFrame:
        """Load a table from SQLite. Optionally extract JSON columns to the selected language."""
        if language is None:
            language = self.language

        df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

        # Detect JSON columns by sample check
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    # Attempt to parse first non-null value
                    sample = df[col].dropna().iloc[0]
                    parsed = json.loads(sample)
                    if isinstance(parsed, dict) and language in parsed:
                        df[col] = df[col].apply(lambda x: json.loads(x).get(language) if pd.notnull(x) else x)
                except Exception:
                    continue
        return df
    
    def upsert_df(self, df: pd.DataFrame, table_name: str, key_columns: List[str]) -> None:
        """
        Insert or update rows from df into table_name based on key_columns.
        """
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                cols = ", ".join(df.columns)
                placeholders = ", ".join([f":{col}" for col in df.columns])
                updates = ", ".join([f"{col}=excluded.{col}" for col in df.columns if col not in key_columns])

                sql = f"""
                    INSERT INTO {table_name} ({cols})
                    VALUES ({placeholders})
                    ON CONFLICT({", ".join(key_columns)}) DO UPDATE SET
                        {updates}
                """
                conn.execute(text(sql), row.to_dict())
