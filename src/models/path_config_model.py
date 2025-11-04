# from pydantic.dataclasses import dataclass
from dataclasses import dataclass  # , asdict


@dataclass
class PathConfiguration:
    logs: str = "../../logs/"
    models: str = "../../sample_models/"
    transformers: str = "../../sample_transformers/"
    output: str = "../../sample_output/"
    configs: str = "../../sample_configs/"
