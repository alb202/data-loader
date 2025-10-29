from utilities.file_readers import read_table
from utilities.read_utilities import validate_path, validate_file


def read_input_data(folder: str, file_name: str) -> dict:
    validated_folder = validate_path(folder)
    validated_file_path = validate_file(validated_folder / file_name)

    return read_table(validated_file_path)
