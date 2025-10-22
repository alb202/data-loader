import toml
import importlib
from pathlib import Path
from typing import Any, Dict


def import_from_string(path: str) -> Any:
    """Dynamically import a class or object from a string like 'module.submodule.Class'."""
    module_name, class_name = path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def load_config(config_path: str | Path) -> Dict[str, Any]:
    """Load TOML configuration file and import the specified classes."""
    config = toml.load(config_path)

    extract_cls = import_from_string(config["extract"]["class"])
    transform_cls = import_from_string(config["transform"]["class"])
    load_cls = import_from_string(config["load"]["class"])

    input_schema = None
    output_schema = None
    if "validation" in config:
        val_cfg = config["validation"]
        if "input_schema" in val_cfg:
            input_schema = import_from_string(val_cfg["input_schema"])
        if "output_schema" in val_cfg:
            output_schema = import_from_string(val_cfg["output_schema"])

    return {
        "pipeline_name": config["pipeline"].get("name", "default_pipeline"),
        "extractor_cls": extract_cls,
        "transformer_cls": transform_cls,
        "loader_cls": load_cls,
        "input_path": config["extract"]["input_path"],
        "output_dest": config["load"]["output_dest"],
        "input_schema": input_schema,
        "output_schema": output_schema,
    }
