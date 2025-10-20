from pathlib import Path
import pandas as pd
from data_loader.load.load_base import Load


def columns_not_in_dataframe(df: pd.DataFrame, columns: list) -> list:
    """Check that all columns are in the dataframe"""
    return [col for col in columns if col not in df]


class ParquetLoader(Load):
    """Save data as partitioned Parquet files."""

    def write(self, df: pd.DataFrame, destination: str, partition_columns: list):
        path = Path(destination)

        missing_partition_columns = columns_not_in_dataframe(df=df, columns=partition_columns)

        if missing_partition_columns:
            raise ValueError(f"Not all partition columns are in DataFrame: {missing_partition_columns}")

        path.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path / "data.parquet", index=False, partition_cols=partition_columns)
