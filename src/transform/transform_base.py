from pandas import DataFrame
from pandera.pandas import DataFrameSchema
from typing import Protocol


class Transform(Protocol):
    """Protocol for transformation classes."""

    @staticmethod
    def transform(
        # cls,
        *dfs: DataFrame,
        output_schema: DataFrameSchema,
    ) -> DataFrame:
        """Apply custom transformation logic."""
        ...

    @staticmethod
    def validate_input(
        # self,
        df: DataFrame,
        input_schema: DataFrameSchema,
    ) -> DataFrame:
        """Validate input DataFrame."""
        return input_schema.validate(df)

    @staticmethod
    def validate_output(
        # self,
        df: DataFrame,
        output_schema: DataFrameSchema,
    ) -> DataFrame:
        """Validate output DataFrame."""
        return output_schema.validate(df)
