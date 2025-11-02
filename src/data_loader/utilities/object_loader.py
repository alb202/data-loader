import importlib.util
import os
from types import ModuleType
from typing import Any
from pathlib import Path


def load_object_from_file(folder_name: Path, file_name: str, object_name: str) -> Any:
    """
    Load and return an attribute (e.g., class, function, variable) from a Python source file.
    Parameters
    ----------
    folder_name : Path
        Directory containing the target Python file.
    file_name : str
        File name of the Python module. The file name may include or omit the ".py" extension;
        the base name (extension removed) is used to derive an internal module name.
    object_name : str
        Name of the attribute to retrieve from the loaded module.
    Returns
    -------
    Any
        The attribute named by `object_name` from the dynamically loaded module.
    Raises
    ------
    FileNotFoundError
        If the constructed file path (folder_name / file_name) does not exist.
    ImportError
        If a module spec cannot be created or the loader is not available for the specified file.
    AttributeError
        If the loaded module does not define an attribute with the given `object_name`.
    Notes
    -----
    - The function loads and executes the module's top-level code using importlib.util.spec_from_file_location
      and the spec's loader; executing the module will run any module-level side effects.
    - A new module object is created and executed; it is not guaranteed to be automatically inserted into sys.modules
      under the derived module name.
    - Use caution when loading and executing untrusted code, as this may introduce security risks.
    """

    file_path: Path = folder_name / file_name

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    module_name: str = os.path.splitext(os.path.basename(file_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import from {file_path}")

    module: ModuleType = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore

    if not hasattr(module, object_name):
        raise AttributeError(f"Object '{object_name}' not found in {file_path}")

    return getattr(module, object_name)
