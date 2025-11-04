# from pydantic.dataclasses import dataclass
from dataclasses import dataclass  # , asdict


@dataclass
class PipelineDetails:
    name: str
    description: str
    transformer_pipeline: str


@dataclass
class InputFile:
    folder: str
    file_name: str
    schema_name: str
    label: str


@dataclass
class OutputTable:
    table_name: str
    schema_name: str
    db: str
    data_label: str


@dataclass
class PipelineConfig:
    extract_files: list[InputFile]
    details: PipelineDetails
    output_table: OutputTable
