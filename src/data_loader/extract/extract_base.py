from typing import Protocol
import pandas as pd
from pathlib import Path


class Extract(Protocol):
    """Protocol for data extraction classes."""

    def read(self, path: Path) -> pd.DataFrame:
        """Read a dataset from a given path and return a DataFrame."""
        ...


# Example extractors:
# extract/csv_extractor.py
# import pandas as pd
# from pathlib import Path
# from .base import Extract
