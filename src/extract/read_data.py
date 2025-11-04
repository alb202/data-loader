from utilities.file_readers import read_table
from utilities.read_utilities import validate_file
from pathlib import Path
from pandas import DataFrame


def read_input_data(path: Path | str) -> DataFrame:
    """
    Read input data from a file after validating its path

    :param path: Path to the input data file
    :type path: Path | str
    :return: DataFrame containing the loaded data
    :rtype: DataFrame
    """

    validated_file_path = validate_file(path=Path(path))

    return read_table(file_path=validated_file_path)
