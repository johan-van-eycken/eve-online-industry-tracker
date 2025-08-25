import os
import json
import pandas as pd
from sqlalchemy import create_engine, text, DDL
from sqlalchemy.orm import sessionmaker
from typing import Optional, List

# ----------------------------
# DatabaseManager
# ----------------------------
class DatabaseManager:
    def __init__(self, db_uri: str, language: str = "en"):
        self.db_uri = db_uri
        self.language = language
        
        # Setup SQLAlchemy Engine and Session
        self.engine = create_engine(self.db_uri)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def get_db_name(self) -> str:
        """ Return the database filename from the URI. Example: 'sqlite:///database/eve_app.db' -> 'eve_app.db' """
        path = self.db_uri
        if path.startswith("sqlite:///"):
            path = path[10:] 
        return os.path.basename(path)

    def save_df(self, df: pd.DataFrame, table_name: str) -> None:
        """Saves a dataframe to a table (using raw SQL for dataframes). Prefer ORM when possible."""
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def load_df(self, table_name: str, language: Optional[str] = None) -> pd.DataFrame:
        """Load contents of a table into a Pandas DataFrame."""
        if language is None:
            language = self.language

        df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

        # Parse JSON columns in the dataframe for the specified language
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    sample = df[col].dropna().iloc[0]  # Check sample for JSON compatibility
                    parsed = json.loads(sample)
                    if isinstance(parsed, dict) and language in parsed:
                        df[col] = df[col].apply(lambda x: json.loads(x).get(language) if pd.notnull(x) else x)
                except Exception:
                    continue
        return df

    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))  # SQLite specific command
            return [row[0] for row in result.fetchall()]

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        stmt = DDL(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine.begin() as conn:
            conn.execute(stmt)