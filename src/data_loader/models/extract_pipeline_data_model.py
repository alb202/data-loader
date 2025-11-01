from dataclasses import dataclass
from pandas import DataFrame
from pandera.pandas import DataFrameSchema


@dataclass
class ExtractPipelineData:
    label: str
    schema: DataFrameSchema
    data: DataFrame
