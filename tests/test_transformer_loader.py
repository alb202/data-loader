import inspect
import pytest
from inspect import Parameter, Signature
from src.utilities.transformer_loader import get_signature, compare_signatures, load_transformer_function, ParameterMismatchError
from pathlib import Path
# import from src.utilities.transformer_loader


def test_get_signature_simple_function():
    def foo(a: int, b: str = "x") -> bool:
        return True

    sig = get_signature(foo)
    assert isinstance(sig, Signature)
    params = list(sig.parameters.values())
    assert [p.name for p in params] == ["a", "b"]
    assert params[0].annotation is int
    assert params[1].annotation is str
    assert sig.return_annotation is bool


def test_get_signature_varargs_and_kwargs():
    def bar(*dfs, **kwargs):
        pass

    sig = get_signature(bar)
    params = list(sig.parameters.values())
    assert len(params) == 2
    assert params[0].kind == Parameter.VAR_POSITIONAL
    assert params[1].kind == Parameter.VAR_KEYWORD
    assert sig.return_annotation is inspect.Signature.empty


def test_get_signature_non_callable_raises():
    # inspect.signature may raise ValueError or TypeError for non-callables depending on Python version
    with pytest.raises((ValueError, TypeError)):
        get_signature(123)


def test_compare_signatures_identical():
    def a(x, y=1) -> int:
        pass

    def b(x, y=1) -> int:
        pass

    sig_a = inspect.signature(a)
    sig_b = inspect.signature(b)
    assert compare_signatures(test_signature=sig_a, template_signature=sig_b) is True


def test_compare_signatures_param_name_mismatch():
    def a(x):
        pass

    def b(y):
        pass

    sig_a = inspect.signature(a)
    sig_b = inspect.signature(b)
    assert compare_signatures(test_signature=sig_a, template_signature=sig_b) is False


def test_compare_signatures_kind_mismatch():
    def a(*dfs, **kwargs):
        pass

    def b(dfs):
        pass

    sig_a = inspect.signature(a)
    sig_b = inspect.signature(b)
    assert compare_signatures(test_signature=sig_a, template_signature=sig_b) is False


def test_compare_signatures_return_annotation_mismatch():
    def a(x) -> int:
        pass

    def b(x) -> str:
        pass

    sig_a = inspect.signature(a)
    sig_b = inspect.signature(b)
    assert compare_signatures(test_signature=sig_a, template_signature=sig_b) is False


def test_compare_signatures_default_mismatch():
    def a(x=1):
        pass

    def b(x=2):
        pass

    sig_a = inspect.signature(a)
    sig_b = inspect.signature(b)
    assert compare_signatures(test_signature=sig_a, template_signature=sig_b) is False


def test_load_transformer_function_success(monkeypatch):
    def template_transform(*dfs, **kwargs):
        pass

    def transformer_transform(*dfs, **kwargs):
        pass

    def mock_loader(folder_name, file_name, object_name):
        if file_name == "template.py":
            return template_transform
        if file_name == "transformer.py":
            return transformer_transform
        raise ValueError("not found")

    monkeypatch.setattr("src.utilities.transformer_loader.load_object_from_file", mock_loader)

    result = load_transformer_function(Path("/path/transformer.py"), Path("/path/template.py"))
    assert result is transformer_transform


def test_load_transformer_function_parameter_mismatch(monkeypatch):
    def template_transform(x):
        pass

    def transformer_transform(x, y=1):
        pass

    def mock_loader(folder_name, file_name, object_name):
        if file_name == "template.py":
            return template_transform
        if file_name == "transformer.py":
            return transformer_transform
        raise ValueError("not found")

    monkeypatch.setattr("src.utilities.transformer_loader.load_object_from_file", mock_loader)

    with pytest.raises(ParameterMismatchError):
        load_transformer_function(Path("/path/transformer.py"), Path("/path/template.py"))


def test_load_transformer_function_template_load_error(monkeypatch):
    def mock_loader(folder_name, file_name, object_name):
        if file_name == "template.py":
            raise ValueError("template load failed")
        return lambda: None

    monkeypatch.setattr("src.utilities.transformer_loader.load_object_from_file", mock_loader)

    with pytest.raises(ValueError, match="Unable to load the signature template"):
        load_transformer_function(Path("/path/transformer.py"), Path("/path/template.py"))


def test_load_transformer_function_transformer_load_error(monkeypatch):
    def template_transform(*args, **kwargs):
        pass

    def mock_loader(folder_name, file_name, object_name):
        if file_name == "template.py":
            return template_transform
        if file_name == "transformer.py":
            raise ValueError("transformer load failed")
        raise ValueError("not found")

    monkeypatch.setattr("src.utilities.transformer_loader.load_object_from_file", mock_loader)

    with pytest.raises(ValueError, match="Unable to load the transformer function"):
        load_transformer_function(Path("/path/transformer.py"), Path("/path/template.py"))


def test_missing_signature_raises(monkeypatch):
    monkeypatch.setattr("src.utilities.transformer_loader.Signature", None)

    with pytest.raises(ValueError, match="Missing signature for transform function"):
        load_transformer_function(Path("/path/transformer.py"), Path("/path/template.py"))
