import os
import json
import pandas as pd  # pyright: ignore[reportMissingModuleSource]
from sqlalchemy import create_engine, text, DDL, event  # pyright: ignore[reportMissingImports]
from sqlalchemy.orm import scoped_session, sessionmaker  # pyright: ignore[reportMissingImports]
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

        # SQLite doesn't support INSERT ... RETURNING reliably → disable
        if self.db_uri.startswith("sqlite"):
            engine_kwargs["implicit_returning"] = False
            engine_kwargs["connect_args"] = {"check_same_thread": False, "timeout": 60}
            # Parallel character refresh can spawn many threads; size the pool
            # large enough to avoid QueuePool exhaustion.
            engine_kwargs["pool_size"] = 20
            engine_kwargs["max_overflow"] = 30

        # Setup SQLAlchemy Engine and Session
        self.engine = create_engine(self.db_uri, **engine_kwargs)

        # Enable WAL journal mode for SQLite to improve concurrent read performance
        # and reduce "database is locked" errors under multi-threaded access.
        if self.db_uri.startswith("sqlite"):
            @event.listens_for(self.engine, "connect")
            def _set_sqlite_pragmas(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=60000")
                cursor.close()

            with self.engine.connect() as conn:
                conn.execute(text("PRAGMA journal_mode=WAL"))
                conn.execute(text("PRAGMA busy_timeout=60000"))
                conn.commit()

        self.Session = sessionmaker(bind=self.engine)
        # Thread-local session proxy. Safe with Flask (multi-threaded) and other callers.
        self.session = scoped_session(self.Session)

    def safe_commit(self) -> None:
        """Commit the current session, rolling back on error."""
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

    def execute(self, query: str, params=None) -> None:
        """Execute a raw SQL query with optional parameters (dict or tuple)."""
        try:
            with self.engine.begin() as conn:
                if params:
                    conn.execute(text(query), params)
                else:
                    conn.execute(text(query))
        except Exception as e:
            raise e

    def query(self, query: str, params=None) -> List:
        """Execute a SELECT query and return results."""
        try:
            with self.engine.begin() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result.fetchall()
        except Exception as e:
            raise e

    def get_db_name(self) -> str:
        """Return the database filename from the URI. Example: 'sqlite:///database/eve_app.db' -> 'eve_app.db'"""
        path = self.db_uri
        if path.startswith("sqlite:///"):
            path = path[10:]
        return os.path.basename(path)

    def save_df(self, df: pd.DataFrame, table_name: str) -> None:
        """Save a DataFrame to a table safely."""

        def query(session, df, table_name):
            df.to_sql(table_name, session.bind, if_exists="replace", index=False)

        self.safe_query(query, df, table_name)

    def load_df(self, table_name: str, language: Optional[str] = None, json_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Load contents of a table into a Pandas DataFrame.

        Args:
            table_name: Name of the table to load.
            language: Language key to extract from JSON columns (e.g. "en").
                      Defaults to the DatabaseManager's configured language.
            json_columns: Explicit list of column names that contain JSON
                          dicts with language keys. Only these columns will
                          be parsed and resolved to the specified language.
                          If None, no JSON parsing is performed.
        """
        if language is None:
            language = self.language

        def query(session, table_name):
            return pd.read_sql_table(table_name, session.bind)

        df = self.safe_query(query, table_name)

        # Parse only explicitly declared JSON columns
        if json_columns:
            for col in json_columns:
                if col not in df.columns:
                    continue
                df[col] = df[col].apply(
                    lambda x: json.loads(x).get(language)
                    if pd.notnull(x) and isinstance(x, str)
                    else x
                )
        return df

    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table';")
            )  # SQLite specific command
            return [row[0] for row in result.fetchall()]

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        stmt = DDL(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine.begin() as conn:
            conn.execute(stmt)