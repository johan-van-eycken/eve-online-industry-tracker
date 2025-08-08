import pandas as pd
from sqlalchemy import create_engine

DB_FILE = "eve_data.db"
engine = create_engine(f"sqlite:///{DB_FILE}")

def save_df(df, table_name):
    df.to_sql(table_name, engine, if_exists="replace", index=False)

def load_df(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)
