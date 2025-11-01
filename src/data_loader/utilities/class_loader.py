"""
dynamic_loader.py

Dynamically load functions, classes, or variables from a Python source file.
Supports both single-use loading and persistent module management with auto-reload.
"""

import importlib.util
from importlib.machinery import ModuleSpec
from types import ModuleType
import sys
import os
# import types
# import time
from pathlib import Path
from utilities.read_utilities import validate_file, validate_path

def load_class_from_file(filepath: str, class_name: str, package_root: str | None = None):
    # Example: filepath="plugins/myplugin/module.py"
    module_name = os.path.splitext(os.path.basename(filepath))[0]
    
    # Optional: if inside a package, define a package path
    if package_root:
        package_name = os.path.basename(package_root)
        full_module_name = f"{package_name}.{module_name}"
        sys.path.insert(0, os.path.dirname(package_root))
    else:
        full_module_name = module_name

    spec = importlib.util.spec_from_file_location(full_module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[full_module_name] = module
    spec.loader.exec_module(module)
    return getattr(module, class_name)


def load_class(full_class_path: str):
    """
    Load a class dynamically, given a full dotted path like:
        'myproject.package.mymodule.MyClass'
    """
    module_path, class_name = full_class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class DynamicClassLoader:
    """ """

    def __init__(self, folder_path: str | Path, file_name: str, class_name: str):
        self.folder_path = validate_path(folder_path)
        self.file_name = file_name
        self.file_path = validate_file(self.folder_path / (file_name + ".py"))
        self.class_name = class_name
        self._class = None
        self._load_module()

    def _load_module(self):
        """Load or reload the module from file."""
        spec: ModuleSpec = importlib.util.spec_from_file_location(
            "transformer_file",
            self.file_path,
        )

        module: ModuleType = importlib.util.module_from_spec(spec=spec)
        # module: ModuleType = importlib.import_module(name=self.file_name + ".py", package=None)

        sys.modules[self.file_name] = module
        spec.loader.exec_module(module=module)

        # Retrieve the class from the module
        if not hasattr(module, self.class_name):
            raise AttributeError(f"Class '{self.class_name}' not found in '{self.file_path}'")

        self._class = getattr(module, self.class_name)
        print(f"Loaded module: {self.file_path}")


# """
# dynamic_loader.py

# Dynamically load functions, classes, or variables from a Python source file.
# Supports both single-use loading and persistent module management with auto-reload.
# """

# import importlib.util
# import sys
# import types
# import time
# from pathlib import Path
# from utilities.read_utilities import validate_file, validate_path


# class DynamicModuleLoader:
#     """
#     Dynamically load and manage Python modules from source files.

#     Features:
#     - Load any symbol (function, class, or variable) by name.
#     - Optional auto-reload if the file changes.
#     """

#     def __init__(self, folder_path: str, file_name: str, class_name: str):
#         self.folder_path = validate_path(folder_path)
#         self.file_name = file_name
#         self.file_path = validate_path(self.folder_path / (file_name + ".py"))
#         self.module_name = class_name
#         # self.auto_reload = auto_reload
#         # self.poll_interval = poll_interval
#         # self._last_mtime = None
#         self._module = None

#         # if not self.file_path.exists():
#         #     raise FileNotFoundError(f"File not found: {self.file_path}")

#         self._load_module()

#         # if self.auto_reload:
#         #     print(f"🔁 Auto-reload enabled for {self.file_path}")
#         #     import threading
#         #     t = threading.Thread(target=self._watch_file, daemon=True)
#         #     t.start()

#     def _load_module(self):
#         """Load or reload the module from file."""
#         spec = importlib.util.spec_from_file_location(self.file_name, self.folder_path)
#         module = importlib.util.module_from_spec(spec)
#         spec.loader.exec_module(module)  # type: ignore

#         print(spec)
#         print(module)

#         self._module = module
#         # self._last_mtime = self.file_path.stat().st_mtime
#         sys.modules[self.module_name] = module
#         print(f"✅ Loaded module: {self.module_name}")

#     # def _watch_file(self):
#     #     """Watch the file and reload if modified."""
#     #     while True:
#     #         time.sleep(self.poll_interval)
#     #         try:
#     #             current_mtime = self.file_path.stat().st_mtime
#     #             if current_mtime != self._last_mtime:
#     #                 print(f"♻️ Reloading {self.file_path} (file changed)")
#     #                 self._load_module()
#     #         except FileNotFoundError:
#     #             time.sleep(self.poll_interval)

#     def get_symbol(self, name: str):
#         """
#         Get a symbol (function, class, or variable) from the loaded module.
#         """
#         if not hasattr(self._module, name):
#             raise AttributeError(f"'{name}' not found in {self.file_path}")
#         return getattr(self._module, name)

#     def list_symbols(self):
#         """Return all user-defined symbols (exclude built-ins)."""
#         return [k for k in dir(self._module) if not k.startswith("_")]
