from data_loader.models.pipeline_config_model import PipelineConfig, PipelineDetails, OutputTable, InputFile

from pathlib import Path
from dataclasses import asdict
import toml


def save_pipeline_config(config: PipelineConfig, file_name: str, config_folder: Path) -> None:
    """Saves a pipeline configuration to a TOML file.
    This function takes a PipelineConfig object and saves it to a TOML file in the specified
    folder with the given filename.
    Args:
        config (PipelineConfig): The pipeline configuration object to save.
        file_name (str): Name of the file to save the configuration to (without extension).
        config_folder (Path): Path to the folder where the configuration file will be saved.
    Returns:
        None
    Example:
        >>> config = PipelineConfig(name="my_pipeline", version="1.0")
        >>> save_pipeline_config(config, "pipeline1", Path("configs/"))
        # Creates configs/pipeline1.toml
    """

    with open(config_folder / f"{file_name}.toml", "w") as f:
        toml.dump(asdict(config), f)


def load_pipeline_config(path: Path) -> PipelineConfig:
    """Loads and validates a pipeline configuration from a TOML file.
    This function reads a TOML configuration file and constructs a PipelineConfig object
    containing the pipeline details, output table configuration, and input file specifications.
    Args:
        path (Path): Path to the TOML configuration file.
    Returns:
        PipelineConfig: A validated pipeline configuration object.
    Raises:
        FileNotFoundError: If the specified configuration file does not exist.
        ValueError: If the configuration file format is invalid or required fields are missing.
    Example:
        >>> config = load_pipeline_config(Path("pipeline_config.toml"))
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
