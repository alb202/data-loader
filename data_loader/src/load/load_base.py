from typing import Protocol
import pandas as pd


class Load(Protocol):
    """Protocol for data loading/output classes."""

    def write(self, df: pd.DataFrame, destination: str):
        """Write a DataFrame to the target destination."""
        ...
