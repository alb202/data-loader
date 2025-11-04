from models.pipeline_config_model import PipelineConfig, PipelineDetails, OutputTable, InputFile

from pathlib import Path
from dataclasses import asdict
import toml


def save_pipeline_config(config: PipelineConfig, file_name: str, config_folder: Path) -> None:
    """
    Save a PipelineConfig object to a TOML file in the specified folder

    :param config: The pipeline configuration to save
    :type config: PipelineConfig
    :param file_name: Name of the TOML file (without extension)
    :type file_name: str
    :param config_folder: Folder where the configuration file will be saved
    :type config_folder: Path
    """
    with open(config_folder / f"{file_name}.toml", "w") as f:
        toml.dump(asdict(config), f)


def load_pipeline_config(path: Path) -> PipelineConfig:
    """
    Load a pipeline configuration from a TOML file and return a PipelineConfig object

    :param path: Path to the TOML configuration file
    :type path: Path
    :return: Loaded PipelineConfig object
    :rtype: PipelineConfig
    """

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, "r") as f:
        config_dict: dict = toml.load(f)

    try:
        details = PipelineDetails(**config_dict["details"])
        output_table = OutputTable(**config_dict["output"])
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
