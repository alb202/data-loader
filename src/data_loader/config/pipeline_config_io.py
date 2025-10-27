from src.data_loader.models.pipeline_config_model import PipelineConfig, PipelineDetails, OutputTable, InputFile

import toml
from pathlib import Path
from dataclasses import asdict


def save_pipeline_config(config: PipelineConfig, file_name: str, config_folder: Path) -> None:
    with open(config_folder / f"{file_name}.toml", "w") as f:
        toml.dump(asdict(config), f)


def load_pipeline_config(path: str) -> PipelineConfig:
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, "r") as f:
        config_dict = toml.load(f)

    try:
        details = PipelineDetails(**config_dict["details"])
        output_table = OutputTable(**config_dict["output_table"])
        extract_files = [InputFile(**input_file) for input_file in config_dict["extract_files"]]
    except (KeyError, ValueError) as e:
        print(e)
        raise ValueError("Error reading lines from configuration file. Check format and try again.")

    try:
        config = PipelineConfig(
            details=details,
            output_table=output_table,
            extract_files=extract_files,
        )
    except KeyError as e:
        print(e)
        raise ValueError("Error trying to import configuration. Check format and try again.")
    return config
