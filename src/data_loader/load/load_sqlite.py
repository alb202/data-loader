import duckdb
import pandas as pd
from data_loader.load.load_base import Load


class DuckDBLoader(Load):
    """Load data into a DuckDB database."""

    def write(self, df: pd.DataFrame, destination: str):
        con = duckdb.connect(destination)
        con.execute("CREATE TABLE IF NOT EXISTS data AS SELECT * FROM df")
        con.close()
