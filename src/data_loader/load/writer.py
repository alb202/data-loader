# import os
# import sys
import duckdb
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, Union, Literal, Callable, Dict

# import pandera as pa
from pandera import DataFrameSchema

# try:
#     import pandera as pa
# except ImportError:
#     pa = None


class DataFrameWriterRegistry:
    """
    Registry for writer backends.
    Allows new writer types to be registered dynamically.
    """

    _writers: Dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a new writer backend."""

        def decorator(func):
            cls._writers[name.lower()] = func
            return func

        return decorator

    @classmethod
    def get_writer(cls, name: str):
        if name.lower() not in cls._writers:
            raise ValueError(f"No writer registered for '{name}'.")
        return cls._writers[name.lower()]

    @classmethod
    def available_writers(cls):
        return list(cls._writers.keys())


class DataFrameWriter:
    """
    A universal, extensible DataFrame writer that supports multiple backends:
      - Partitioned Parquet files
      - DuckDB databases
      - SQLite databases
      - Tab-delimited text files
    and allows dynamic registration of new backends.

    Optionally validates the DataFrame using a Pandera schema before writing.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        output_path: Union[str, Path],
        write_method: str,
        db: str,
        table_name: Optional[str] = None,
        partition_cols: Optional[list[str]] = None,
        schema: Optional[DataFrameSchema] = None,
        mode: Literal["overwrite", "append"] = "overwrite",
        validate: bool = True,
        **kwargs,
    ):
        self.df = df
        self.output_path = Path(output_path)
        self.write_method = write_method.lower()
        self.table_name = table_name
        self.db = db
        self.partition_cols = partition_cols or []
        self.schema = schema
        self.mode = mode
        self.validate = validate
        self.kwargs = kwargs

        # Create output directories if needed
        if self.write_method == "parquet" and not self.output_path.exists():
            self.output_path.mkdir(parents=True, exist_ok=True)

    # -------------------------------
    # Public write method
    # -------------------------------
    def write(self):
        """Validate (optional) and write the dataframe using the chosen backend."""
        if self.validate and self.schema is not None:
            self._validate_with_pandera()

        writer_func = DataFrameWriterRegistry.get_writer(self.write_method)
        writer_func(self)  # Pass the instance to the writer function

    # -------------------------------
    # Schema validation
    # -------------------------------
    def _validate_with_pandera(self):
        # """Validate the dataframe using a Pandera schema, if available."""
        # if pa is None:
        #     raise ImportError("Pandera is not installed. Install it with `pip install pandera`.")

        if isinstance(self.schema, DataFrameSchema):
            self.df = self.schema.validate(self.df)
        else:
            raise TypeError("Schema must be a pandera DataFrameSchema")

        print("DataFrame validated successfully with Pandera schema.")


# ======================================================
# Default Writer Backends
# ======================================================


@DataFrameWriterRegistry.register("parquet")
def write_parquet(writer: DataFrameWriter):
    """Write to partitioned or flat Parquet files."""
    df = writer.df
    output_path = writer.output_path
    table_name = writer.table_name

    partition_cols = writer.partition_cols
    db = writer.db

    # output_path = Path(output_path) / db / (table_name + ".parquet")

    if partition_cols:
        output_path = Path(output_path) / db / (table_name)
        df.to_parquet(output_path, partition_cols=partition_cols, index=False)
    else:
        output_path = Path(output_path) / db / (table_name + ".parquet")
        # file_path = output_path / "table_name.parquet" if output_path.is_dir() else output_path
        df.to_parquet(output_path, index=False)

    print(f"Wrote Parquet data to: {output_path}")


@DataFrameWriterRegistry.register("duckdb")
def write_duckdb(writer: DataFrameWriter):
    """Write to a DuckDB database."""

    df = writer.df
    output_path = writer.output_path
    table_name = writer.table_name
    mode = writer.mode
    db = writer.db

    if not table_name:
        raise ValueError("A table_name is required for DuckDB writes.")

    output_path = Path(output_path) / (db + ".duckdb")

    conn = duckdb.connect(str(output_path))
    if mode == "overwrite":
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.register("tmp_df", df)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_df")
    if mode == "append":
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df")

    conn.close()
    print(f"Wrote DuckDB table: {table_name} in {output_path}")


@DataFrameWriterRegistry.register("sqlite")
def write_sqlite(writer: DataFrameWriter):
    """Write to a SQLite database."""
    df = writer.df
    output_path = writer.output_path
    table_name = writer.table_name
    mode = writer.mode
    db = writer.db

    if not table_name:
        raise ValueError("A table_name is required for SQLite writes.")

    output_path = Path(output_path) / (db + ".sqlite")

    conn = sqlite3.connect(output_path)
    df.to_sql(
        table_name,
        conn,
        if_exists="replace" if mode == "overwrite" else "append",
        index=False,
    )
    conn.close()
    print(f"Wrote SQLite table: {table_name} in {output_path}")


@DataFrameWriterRegistry.register("tsv")
def write_tsv(writer: DataFrameWriter):
    """Write to tab-delimited text file."""
    df, output_path = writer.df, writer.output_path
    file_path = output_path / "data.tsv" if output_path.is_dir() else output_path
    df.to_csv(file_path, sep="\t", index=False)
    print(f"Wrote TSV file: {file_path}")


# import pandas as pd
# import pandera as pa
# from pandera import Column, DataFrameSchema, Check
# from dataframe_writer import DataFrameWriter, DataFrameWriterRegistry

# # Example schema
# sales_schema = DataFrameSchema({
#     "region": Column(str, Check.isin(["US", "CA", "UK"])),
#     "year": Column(int, Check.ge(2000)),
#     "sales": Column(float, Check.ge(0))
# })

# df = pd.DataFrame({
#     "region": ["US", "CA", "US"],
#     "year": [2023, 2024, 2025],
#     "sales": [100.0, 200.5, 300.7]
# })

# # Write to Parquet
# DataFrameWriter(
#     df=df,
#     output_path="output/parquet_data",
#     write_method="parquet",
#     partition_cols=["region"],
#     schema=sales_schema,
# ).write()

# # Dynamically check what writers are available
# print("Available writers:", DataFrameWriterRegistry.available_writers())

# # Add a custom writer dynamically
# @DataFrameWriterRegistry.register("csv")
# def write_csv(writer):
#     writer.df.to_csv(writer.output_path, index=False)
#     print(f"Wrote CSV to {writer.output_path}")

# # Use the custom writer
# DataFrameWriter(df, "output/custom.csv", "csv").write()
