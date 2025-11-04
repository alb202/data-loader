# from src.utilities.file_readers import read_table, detect_file_type, UnsupportedFileTypeError
# from src.models.pipeline_config_model import PipelineConfig, PipelineDetails, OutputTable, InputFile

# import gzip

# import json
# import pandas as pd
# import pytest


# @pytest.fixture
# def sample_dataframe():
#     """A small sample dataframe for testing."""
#     return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10.5, 20.3, 30.1]})


# def test_validate_config(tmp_path):
#     file_path = tmp_path / "data.csv"
#     file_path.write_text("a,b,c\n1,2,3\n")
#     assert detect_file_type(file_path) == "csv"


# def test_detect_file_type_tsv(tmp_path):
#     file_path = tmp_path / "data.tsv"
#     file_path.write_text("a\tb\tc\n1\t2\t3\n")
#     assert detect_file_type(file_path) == "tsv"


# def test_detect_file_type_excel(tmp_path):
#     file_path = tmp_path / "data.xlsx"
#     file_path.touch()
#     assert detect_file_type(file_path) == "excel"


# def test_load_csv(tmp_path, sample_dataframe):
#     file_path = tmp_path / "data.csv"
#     sample_dataframe.to_csv(file_path, index=False)

#     df = read_table(file_path)
#     assert len(df) == 3
#     assert list(df.columns) == ["id", "name", "value"]


# def test_load_tsv(tmp_path, sample_dataframe):
#     file_path = tmp_path / "data.tsv"
#     sample_dataframe.to_csv(file_path, index=False, sep="\t")

#     df = read_table(file_path)
#     assert df.equals(sample_dataframe)


# def test_load_csv_gz(tmp_path, sample_dataframe):
#     file_path = tmp_path / "data.csv.gz"
#     with gzip.open(file_path, "wt", encoding="utf-8") as f:
#         sample_dataframe.to_csv(f, index=False)

#     df = read_table(file_path)
#     assert len(df) == 3
#     assert set(df.columns) == {"id", "name", "value"}


# def test_load_parquet(tmp_path, sample_dataframe):
#     file_path = tmp_path / "data.parquet"
#     sample_dataframe.to_parquet(file_path)

#     df = read_table(file_path)
#     assert df.equals(sample_dataframe)


# def test_load_excel(tmp_path, sample_dataframe):
#     file_path = tmp_path / "data.xlsx"
#     sample_dataframe.to_excel(file_path, index=False)

#     df = read_table(file_path)
#     assert len(df) == 3
#     assert "name" in df.columns


# def test_load_json(tmp_path, sample_dataframe):
#     file_path = tmp_path / "data.json"
#     sample_dataframe.to_json(file_path)
#     # sample_dataframe.to_json(file_path, orient="records", lines=True)

#     try:
#         with open(file_path, "r") as f:
#             tmp = json.load(f)
#         # df = read_table(file_path)
#         assert len(tmp.keys()) == 3
#         assert "id" in list(tmp.keys())
#         assert "name" in list(tmp.keys())
#         assert "value" in list(tmp.keys())
#     except Exception:
#         raise


# def test_load_feather(tmp_path, sample_dataframe):
#     file_path = tmp_path / "data.feather"
#     sample_dataframe.to_feather(file_path)

#     df = read_table(file_path)
#     assert df.equals(sample_dataframe)


# def test_unsupported_file_type(tmp_path):
#     file_path = tmp_path / "data.unknown"
#     file_path.write_text("random text")

#     with pytest.raises(UnsupportedFileTypeError):
#         _ = read_table(file_path)


# def test_file_not_found():
#     with pytest.raises(FileNotFoundError):
#         _ = read_table("nonexistent.csv")


# from src.data_loader.utilities.read_utilities import validate_file, validate_path
# import pytest


# def test_validate_path_true(tmp_path):
#     assert validate_path(str(tmp_path)) == tmp_path


# def test_validate_path_false(tmp_path):
#     with pytest.raises(NotADirectoryError):
#         validate_path(str(tmp_path / "nonexistant_dir"))


# def test_validate_file_true(tmp_path):
#     file_path = tmp_path / "data.tsv"
#     file_path.write_text("a\tb\tc\n1\t2\t3\n")
#     assert validate_file(str(file_path)) == file_path


# def test_validate_file_false(tmp_path):
#     with pytest.raises(FileNotFoundError):
#         validate_file(str(tmp_path / "nonexistant_file.tsv"))
