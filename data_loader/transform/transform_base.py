import pandas as pd
import pandera as pa
from typing import Protocol, Type, Optional


class Transform(Protocol):
    """Protocol for transformation classes."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply custom transformation logic."""
        ...

    def validate_input(
        self,
        df: pd.DataFrame,
        schema: Optional[Type[pa.DataFrameModel]] = None,
    ) -> pd.DataFrame:
        """Validate input DataFrame."""
        if schema:
            df = schema.validate(df)
        return df

    def validate_output(
        self,
        df: pd.DataFrame,
        schema: Optional[Type[pa.DataFrameModel]] = None,
    ) -> pd.DataFrame:
        """Validate output DataFrame."""
        if schema:
            df = schema.validate(df)
        return df
