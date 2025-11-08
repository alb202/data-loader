from data_loader.file_type_readers import validate_file, validate_path
import pytest


def test_validate_path_true(tmp_path):
    assert validate_path(str(tmp_path)) == tmp_path


def test_validate_path_false(tmp_path):
    with pytest.raises(NotADirectoryError):
        validate_path(str(tmp_path / "nonexistant_dir"))


def test_validate_file_true(tmp_path):
    file_path = tmp_path / "data.tsv"
    file_path.write_text("a\tb\tc\n1\t2\t3\n")
    assert validate_file(str(file_path)) == file_path


def test_validate_file_false(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_file(str(tmp_path / "nonexistant_file.tsv"))
