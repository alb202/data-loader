from data_loader.object_loader import load_object_from_file

import pytest
from pathlib import Path
from importlib.machinery import ModuleSpec


def _write_module(tmp_path: Path, content: str, filename: str = "mod.py") -> Path:
    """
    Helper to write a small python module into the temporary directory.
    """
    path = tmp_path / filename
    path.write_text(content)
    return path


def test_load_function_success(tmp_path):
    _write_module(tmp_path, "def hello():\n    return 'hi'\n")
    obj = load_object_from_file(tmp_path, "mod.py", "hello")
    assert callable(obj)
    assert obj() == "hi"


def test_load_class_and_variable(tmp_path):
    content = "class C:\n    def __init__(self):\n        self.x = 1\n\nvar = 42\n"
    _write_module(tmp_path, content)
    C = load_object_from_file(tmp_path, "mod.py", "C")
    var = load_object_from_file(tmp_path, "mod.py", "var")
    assert isinstance(C(), object)
    assert getattr(C(), "x") == 1
    assert var == 42


def test_file_not_found(tmp_path):
    # no file created; should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        load_object_from_file(tmp_path, "no_such.py", "anything")


def test_attribute_not_found(tmp_path):
    # module exists but object is missing
    _write_module(tmp_path, "a = 1\n")
    with pytest.raises(AttributeError):
        load_object_from_file(tmp_path, "mod.py", "missing")


def test_import_error_when_spec_loader_none(monkeypatch, tmp_path):
    # create a file so the existence check passes
    _write_module(tmp_path, "a = 1\n")

    # simulate spec with loader == None
    def fake_spec(name, path):
        return ModuleSpec(name, loader=None)

    monkeypatch.setattr("importlib.util.spec_from_file_location", fake_spec)

    with pytest.raises(ImportError):
        load_object_from_file(tmp_path, "mod.py", "a")


def test_import_error_when_spec_none(monkeypatch, tmp_path):
    # create a file so the existence check passes
    _write_module(tmp_path, "a = 1\n")

    # simulate spec_from_file_location returning None
    monkeypatch.setattr("importlib.util.spec_from_file_location", lambda name, path: None)

    with pytest.raises(ImportError):
        load_object_from_file(tmp_path, "mod.py", "a")
