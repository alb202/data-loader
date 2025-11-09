from data_loader.object_loader import load_object_from_file
from pathlib import Path
from inspect import Signature, Parameter, signature
from pandas import DataFrame
from typing import Any, Callable


class ParameterMismatchError(Exception):
    """Raised when the file type is not recognized or supported."""

    pass


class ReturnAnnotationMismatchError(Exception):
    """Raised when the file type is not recognized or supported."""

    pass


def get_signature(obj: Any) -> Signature:
    """
    Retrieves the signature of a callable object.

    Args:
        obj (Any): A callable object (function, method, class, etc.) whose signature needs to be inspected.

    Returns:
        Signature: A Signature object containing information about the parameters and return annotation of the callable.

    Raises:
        TypeError: If the object is not callable.
        ValueError: If the signature cannot be determined.

    Example:
        >>> def foo(x: int, y: str = 'default') -> bool:
        ...     pass
        >>> sig = get_signature(foo)
        >>> print(sig)
        (x: int, y: str = 'default') -> bool
    """
    return signature(obj)


def default_signature() -> Signature:
    param_args: Parameter = Parameter(name="dfs", kind=Parameter.VAR_POSITIONAL, annotation=DataFrame)
    param_kwargs: Parameter = Parameter(name="kwargs", kind=Parameter.VAR_KEYWORD)

    return Signature(parameters=[param_args, param_kwargs], return_annotation=DataFrame)


def compare_signatures(test_signature: Signature, template_signature: Signature) -> bool:
    """
    Compare two Signature objects for exact equality.

    This function returns True if `test_signature` and `template_signature` are
    considered equal according to the Signature object's equality semantics,
    and False otherwise. Equality is determined by the Signature type's
    implementation (for example, inspect.Signature compares parameter names,
    kinds, default values and annotations, and the return annotation).

    Parameters
    ----------
    test_signature : Signature
        The signature to be tested.
    template_signature : Signature
        The signature to compare against (the expected/template signature).

    Returns
    -------
    bool
        True if the two signatures are equal, False otherwise.

    Notes
    -----
    - The exact behavior of equality depends on the Signature class in use.
      When using inspect.Signature, parameter order, kinds (POSITIONAL_ONLY,
      VAR_POSITIONAL, KEYWORD_ONLY, VAR_KEYWORD), defaults and annotations are
      part of the comparison.
    - This function performs a direct equality check and does not attempt any
      normalization or compatibility matching (e.g., it does not consider a
      subset of parameters to be a match).

    Example
    -------

    def func_a(x, y=1) -> int: ...
    def func_b(x, y=1) -> int: ...

    compare_signatures(signature(func_a), signature(func_b))  # True
    """
    return test_signature == template_signature


def load_transformer_function(transformer_file: Path, template_file: Path) -> Callable:
    """Load and validate a transformer function from a file against a template.
    This function loads a transformer function from a specified file and validates its signature
    against a template function signature. Both functions must be named 'transform'.
    Args:
        transformer_file (Path): Path to the file containing the transformer function.
        template_file (Path): Path to the file containing the template function.
    Returns:
        Callable: The loaded transformer function if signature validation passes.
    Raises:
        ValueError: If either the template or transformer function cannot be loaded.
        ParameterMismatchError: If the transformer function's signature doesn't match the template.
    Example:
        >>> transformer = load_transformer_function(
        ...     Path("my_transformer.py"),
        ...     Path("template.py")
        ... )
        >>> result = transformer(data)
    """

    try:
        template_signature = get_signature(
            obj=load_object_from_file(folder_name=template_file.parent, file_name=template_file.name, object_name="transform")
        )
    except ValueError:
        raise ValueError("Unable to load the signature template. Check the file and try again.")

    try:
        transformer_function = load_object_from_file(
            folder_name=transformer_file.parent, file_name=transformer_file.name, object_name="transform"
        )
    except ValueError:
        raise ValueError("Unable to load the transformer function. Check the file and try again.")

    transformer_signature = get_signature(transformer_function)

    if compare_signatures(test_signature=transformer_signature, template_signature=template_signature):
        return transformer_function
    else:
        raise ParameterMismatchError(f"Function signature mismatch.\nExpected: {template_signature}\nGot:      {transformer_signature}")
