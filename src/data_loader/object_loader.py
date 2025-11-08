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


# """
# Utilities for dynamic loading of Python objects from a file.

# This module exposes `load_object_from_file`, a helper to import a Python module
# from a file path and retrieve a named attribute (class, function, variable, etc.)
# from it.

# Warning:
#     Importing a module from a file executes top-level code in that file. Only use
#     this function with trusted files. The function may also be affected by module
#     name collisions in the current Python process.
# """

# import os
# import importlib.util
# from types import ModuleType
# from pathlib import Path
# from typing import Any

# def load_object_from_file(folder_name: Path, file_name: str, object_name: str) -> Any:
#     """
#     Load and return an attribute named `object_name` from a Python source file
#     located at `folder_name / file_name`.

#     This function:
#     - Builds the filesystem path to the target Python file.
#     - Verifies the file exists.
#     - Creates an import spec for that file and builds a module from the spec.
#     - Executes the module so its top-level code runs (this is equivalent to an
#       import).
#     - Retrieves and returns the attribute named `object_name` from the loaded module.

#     Parameters
#     ----------
#     folder_name : Path
#         Directory containing the Python file (for example `Path('plugins')`).
#     file_name : str
#         File name (for example `'my_plugin.py'`). Should include the `.py` suffix.
#     object_name : str
#         Name of the attribute (class, function, variable, etc.) to retrieve from
#         the imported module.

#     Returns
#     -------
#     Any
#         The attribute named `object_name` from the imported module. The exact
#         type depends on what was stored under that name in the file.

#     Raises
#     ------
#     FileNotFoundError
#         If the computed file path (`folder_name / file_name`) does not exist.
#     ImportError
#         If a module spec or loader cannot be created for the file (import fails).
#     AttributeError
#         If the module does not contain an attribute with the name `object_name`.

#     Notes
#     -----
#     - Importing the module will execute any top-level statements in the file.
#       Treat the file as untrusted input only with extreme caution.
#     - The derived module name is the base file name (without extension). This
#       can cause name collisions if multiple modules with the same base name are
#       loaded in the same process. If you need to avoid collisions, consider
#       generating a unique module name (e.g. appending a UUID) before creating
#       a spec.
#     - The function returns the raw attribute; callers are responsible for
#       instantiating classes or invoking callables as needed.

#     Example
#     -------
#     >>> from pathlib import Path
#     >>> MyClass = load_object_from_file(Path('plugins'), 'plugin_example.py', 'Plugin')
#     >>> instance = MyClass()

#     """
#     # Compose the full file path from the given folder and file name.
#     file_path: Path = folder_name / file_name

#     # Check that the target Python file exists on disk.
#     if not os.path.exists(file_path):
#         # Raise an explicit error so callers can distinguish this case.
#         raise FileNotFoundError(f"File not found: {file_path}")

#     # Derive a module name from the file's base name (without extension).
#     # Note: this simple name can lead to collisions if different files share the
#     # same base name.
#     module_name: str = os.path.splitext(os.path.basename(file_path))[0]

#     # Create an import spec for the module located at the given file path.
#     spec = importlib.util.spec_from_file_location(module_name, file_path)
#     # If we couldn't create a spec or a loader, importing is not possible.
#     if spec is None or spec.loader is None:
#         raise ImportError(f"Cannot import from {file_path}")

#     # Create a new module object from the spec. This does not yet execute the module.
#     module: ModuleType = importlib.util.module_from_spec(spec)

#     # Execute the module in its own namespace. This runs top-level code in the file.
#     # The `# type: ignore` is used because static type checkers may not know the
#     # specific loader type exposing `exec_module`.
#     spec.loader.exec_module(module)  # type: ignore

#     # Ensure the requested attribute exists on the imported module.
#     if not hasattr(module, object_name):
#         raise AttributeError(f"Object '{object_name}' not found in {file_path}")

#     # Return the attribute (could be a class, function, constant, etc.).
#     return getattr(module, object_name)
