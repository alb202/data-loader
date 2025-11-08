import pytest

# from pathlib import Path
import toml

# import tempfile
# import os
from config.pipeline_config_io import load_pipeline_config
from models.pipeline_config_model import (
    PipelineConfig,
    # PipelineDetails,
    # OutputTable,
    # InputFile,
)  # , PipelineDetails, OutputTable, InputFile


def make_toml_file(tmp_path, data):
    file_path = tmp_path / "test_config.toml"
    with open(file_path, "w") as f:
        toml.dump(data, f)
    return file_path


def valid_config_dict():
    return {
        "details": {"name": "TestPipeline", "description": "A test pipeline", "project_path": "./", "transformer_pipeline": "pipeline"},
        "output": {
            "schema_file": "schemas/output_schema.py",
            "output_path": "/test/output_folder/",
            "table_name": "output_table",
            "db": "public",
            "data_label": "data_label",
        },
        "extract_files": [
            {"data_file": "/data/input1.csv", "schema_file": "schemas/schema_file1.py", "label": "test_label1"},
            {"data_file": "/data/input2.parquet", "schema_file": "schemas/schema_file2.py", "label": "test_label2"},
            {"data_file": "/data/input3.tsv", "schema_file": "schemas/schema_file3.py", "label": "test_label3"},
        ],
    }


def test_load_pipeline_config_success(tmp_path):
    config_dict = valid_config_dict()
    file_path = make_toml_file(tmp_path, config_dict)
    config = load_pipeline_config(file_path)
    assert isinstance(config, PipelineConfig)
    assert config.details.name == "TestPipeline"
    assert config.output_table.table_name == "output_table"
    assert len(config.extract_files) == 3
    assert config.extract_files[0].data_file == "/data/input1.csv"
    assert config.extract_files[1].data_file == "/data/input2.parquet"
    assert config.extract_files[2].data_file == "/data/input3.tsv"


def test_load_pipeline_config_file_not_found(tmp_path):
    file_path = tmp_path / "nonexistent.toml"
    with pytest.raises(FileNotFoundError):
        load_pipeline_config(file_path)


def test_load_pipeline_config_missing_details(tmp_path):
    config_dict = valid_config_dict()
    del config_dict["details"]
    file_path = make_toml_file(tmp_path, config_dict)
    with pytest.raises(ValueError):
        load_pipeline_config(file_path)


def test_load_pipeline_config_missing_output(tmp_path):
    config_dict = valid_config_dict()
    del config_dict["output"]
    file_path = make_toml_file(tmp_path, config_dict)
    with pytest.raises(ValueError):
        load_pipeline_config(file_path)


def test_load_pipeline_config_missing_extract_files(tmp_path):
    config_dict = valid_config_dict()
    del config_dict["extract_files"]
    file_path = make_toml_file(tmp_path, config_dict)
    with pytest.raises(ValueError):
        load_pipeline_config(file_path)


def test_load_pipeline_config_invalid_extract_files(tmp_path):
    config_dict = valid_config_dict()
    config_dict["extract_files"] = None
    file_path = make_toml_file(tmp_path, config_dict)
    with pytest.raises(ValueError):
        load_pipeline_config(file_path)
