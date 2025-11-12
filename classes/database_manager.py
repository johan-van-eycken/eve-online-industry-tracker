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

        # Engine kwargs
        engine_kwargs = dict(echo=False, future=True)
        
        # SQLite doesn’t support INSERT ... RETURNING reliably → disable
        if self.db_uri.startswith("sqlite"):
            engine_kwargs["implicit_returning"] = False
        
        # Setup SQLAlchemy Engine and Session
        self.engine = create_engine(self.db_uri, **engine_kwargs)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def safe_commit(self) -> None:
        """ Commit the current session, rolling back on error. """
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        
    def safe_query(self, query_fn, *args, **kwargs):
        """ 
        Execute a query function safely, rolling back the session if an exception occurs.
        query_fn should be a function that takes the session as its first argument.
        """
        try:
            return query_fn(self.session, *args, **kwargs)
        except Exception as e:
            self.session.rollback()
            raise e

    def get_db_name(self) -> str:
        """ Return the database filename from the URI. Example: 'sqlite:///database/eve_app.db' -> 'eve_app.db' """
        path = self.db_uri
        if path.startswith("sqlite:///"):
            path = path[10:] 
        return os.path.basename(path)

    def save_df(self, df: pd.DataFrame, table_name: str) -> None:
        """Save a DataFrame to a table safely."""
        def query(session, df, table_name):
            df.to_sql(table_name, session.bind, if_exists='replace', index=False)
        self.safe_query(query, df, table_name)

    def load_df(self, table_name: str, language: Optional[str] = None) -> pd.DataFrame:
        """Load contents of a table into a Pandas DataFrame."""
        if language is None:
            language = self.language

        def query(session, table_name):
            return pd.read_sql_table(table_name, session.bind)
        df = self.safe_query(query, table_name)

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