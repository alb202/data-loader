from utilities.object_loader import load_object_from_file
from pathlib import Path
from inspect import Signature, Parameter, signature
from pandas import DataFrame
from typing import Any, Callable

# from typing import Callable  # Union,
# from inspect import Signature
# from pandera.pandas import DataFrameSchema


class ParameterMismatchError(Exception):
    """Raised when the file type is not recognized or supported."""

    pass


class ReturnAnnotationMismatchError(Exception):
    """Raised when the file type is not recognized or supported."""

    pass


def get_signature(obj: Any) -> Signature:
    return signature(obj)


def default_signature() -> Signature:
    param_args: Parameter = Parameter(name="dfs", kind=Parameter.VAR_POSITIONAL, annotation=DataFrame)
    param_kwargs: Parameter = Parameter(name="kwargs", kind=Parameter.VAR_KEYWORD)

    return Signature(parameters=[param_args, param_kwargs], return_annotation=DataFrame)


def compare_signatures(test_signature: Signature, template_signature: Signature) -> bool:
    if test_signature == template_signature:
        return True
    else:
        return False


def load_transformer_function(transformer_file: Path, template_file: Path) -> Callable:
    if not Signature:
        raise ValueError("Missing signature for transform function")

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

    # if list(test_signature.parameters.keys()) != list(template_signature.parameters.keys()):
    #     return False

    # if template_signature.return_annotation not in (Signature.empty, None):
    #     return False

    # return True


# def compare_signatures(user_signature, template_signature) -> str | None:

#     if list(user_signature.parameters.keys()) != list(template_signature.parameters.keys()):
#         raise ParameterMismatchError(
#             f"Function parameter mismatch.\n"
#             f"Expected: {list(template_signature.parameters.keys())}\n"
#             f"Got:      {list(user_signature.parameters.keys())}"
#         )

#     if template_signature.return_annotation not in (Signature.empty, None):
#         if user_signature.return_annotation != template_signature.return_annotation:
#             raise ReturnAnnotationMismatchError(
#                 f"Warning: Return annotation does not match template. \n"
#                 f"Expected: {template_signature.return_annotation}\n"
#                 f"Got:      {user_signature.return_annotation}"
#             )

#     return True
