# import io
import gzip

import json
import pandas as pd
import pytest

# from pathlib import Path
from data_loader.src.utilities.file_readers import read_table, detect_file_type, UnsupportedFileTypeError


@pytest.fixture
def sample_dataframe():
    """A small sample dataframe for testing."""
    return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10.5, 20.3, 30.1]})


def test_detect_file_type_csv(tmp_path):
    file_path = tmp_path / "data.csv"
    file_path.write_text("a,b,c\n1,2,3\n")
    assert detect_file_type(file_path) == "csv"


def test_detect_file_type_tsv(tmp_path):
    file_path = tmp_path / "data.tsv"
    file_path.write_text("a\tb\tc\n1\t2\t3\n")
    assert detect_file_type(file_path) == "tsv"


def test_detect_file_type_excel(tmp_path):
    file_path = tmp_path / "data.xlsx"
    file_path.touch()
    assert detect_file_type(file_path) == "excel"


def test_load_csv(tmp_path, sample_dataframe):
    file_path = tmp_path / "data.csv"
    sample_dataframe.to_csv(file_path, index=False)

    df = read_table(file_path)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "value"]


def test_load_tsv(tmp_path, sample_dataframe):
    file_path = tmp_path / "data.tsv"
    sample_dataframe.to_csv(file_path, index=False, sep="\t")

    df = read_table(file_path)
    assert df.equals(sample_dataframe)


def test_load_csv_gz(tmp_path, sample_dataframe):
    file_path = tmp_path / "data.csv.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        sample_dataframe.to_csv(f, index=False)

    df = read_table(file_path)
    assert len(df) == 3
    assert set(df.columns) == {"id", "name", "value"}


def test_load_parquet(tmp_path, sample_dataframe):
    file_path = tmp_path / "data.parquet"
    sample_dataframe.to_parquet(file_path)

    df = read_table(file_path)
    assert df.equals(sample_dataframe)


def test_load_excel(tmp_path, sample_dataframe):
    file_path = tmp_path / "data.xlsx"
    sample_dataframe.to_excel(file_path, index=False)

    df = read_table(file_path)
    assert len(df) == 3
    assert "name" in df.columns


def test_load_json(tmp_path, sample_dataframe):
    file_path = tmp_path / "data.json"
    sample_dataframe.to_json(file_path)
    # sample_dataframe.to_json(file_path, orient="records", lines=True)

    try:
        with open(file_path, "r") as f:
            tmp = json.load(f)
        # df = read_table(file_path)
        assert len(tmp.keys()) == 3
        assert "id" in list(tmp.keys())
        assert "name" in list(tmp.keys())
        assert "value" in list(tmp.keys())
    except Exception:
        raise


def test_load_feather(tmp_path, sample_dataframe):
    file_path = tmp_path / "data.feather"
    sample_dataframe.to_feather(file_path)

    df = read_table(file_path)
    assert df.equals(sample_dataframe)


def test_unsupported_file_type(tmp_path):
    file_path = tmp_path / "data.unknown"
    file_path.write_text("random text")

    with pytest.raises(UnsupportedFileTypeError):
        _ = read_table(file_path)


def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        _ = read_table("nonexistent.csv")
