from pandas import DataFrame
from pandera.pandas import DataFrameSchema
from typing import Protocol


class Transform(Protocol):
    """Protocol for transformation classes."""

    def transform(self, *dfs: DataFrame, output_schema: DataFrameSchema) -> DataFrame:
        """Apply custom transformation logic."""
        ...

    def validate_input(
        self,
        df: DataFrame,
        schema: DataFrameSchema,
    ) -> DataFrame:
        """Validate input DataFrame."""
        return schema.validate(df)

    def validate_output(
        self,
        df: DataFrame,
        schema: DataFrameSchema,
    ) -> DataFrame:
        """Validate output DataFrame."""
        return schema.validate(df)
