from dataclasses import dataclass


@dataclass
class PipelineDetails:
    project_path: str
    name: str
    description: str
    transformer_pipeline: str


@dataclass
class InputFile:
    data_file: str
    schema_file: str
    label: str


@dataclass
class OutputTable:
    schema_file: str
    output_path: str
    table_name: str
    db: str
    data_label: str


@dataclass
class PipelineConfig:
    extract_files: list[InputFile]
    details: PipelineDetails
    output_table: OutputTable
