import importlib.util
import os
from types import ModuleType
from typing import Any
from pathlib import Path


def load_object_from_file(folder_name: Path, file_name: str, object_name: str) -> Any:
    """
    Dynamically loads an object (e.g., 'schema') from a Python file at runtime.

    Parameters
    ----------
    file_path : str
        The path to the Python file containing the object.
    object_name : str, optional
        The name of the object to load (default is 'schema').

    Returns
    -------
    Any
        The requested object from the module.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    AttributeError
        If the specified object is not found in the module.
    """
    file_path = folder_name / file_name

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    module_name = os.path.splitext(os.path.basename(file_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import from {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore

    if not hasattr(module, object_name):
        raise AttributeError(f"Object '{object_name}' not found in {file_path}")

    return getattr(module, object_name)
