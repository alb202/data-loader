import sqlite3
import pandas as pd
from data_loader.load.load_base import Load


class SQLiteLoader(Load):
    """Load data into a SQLite database."""

    def write(self, df: pd.DataFrame, destination: str):
        con = sqlite3.connect(destination)
        df.to_sql("data", con, if_exists="replace", index=False)
        con.close()
