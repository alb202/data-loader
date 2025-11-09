from main import run_pipeline, cli
import pytest
from unittest.mock import patch, Mock  # , MagicMock


@pytest.fixture
def mock_config_dict():
    mock = Mock()
    mock.details = Mock()
    mock.details.project_path = "/test/path"
    mock.details.name = "Test Pipeline"
    mock.details.description = "Test Description"
    mock.details.transformer_pipeline = "transform.py"

    mock.extract_files = [Mock()]
    mock.extract_files[0].data_file = "test.csv"
    mock.extract_files[0].schema_file = "schema.py"
    mock.extract_files[0].label = "test_data"

    mock.output_table = Mock()
    mock.output_table.schema_file = "output_schema.py"
    mock.output_table.output_path = "/output/path"
    mock.output_table.db = "test_db"
    mock.output_table.table_name = "test_table"
    mock.output_table.data_label = "test_label"

    return mock


@pytest.mark.parametrize("dry_run", [True, False])
def test_run_pipeline(mock_config_dict, dry_run):
    with (
        patch("main.load_pipeline_config") as mock_load_config,
        patch("main.setup_logger") as mock_logger,
        patch("main.read_input_data") as mock_read_data,
        patch("main.load_object_from_file") as mock_load_object,
        patch("main.load_transformer_function") as mock_load_transformer,
        patch("main.DataFrameWriter") as mock_writer,
    ):
        # Setup mocks
        mock_load_config.return_value = mock_config_dict
        mock_logger.return_value = Mock()
        mock_read_data.return_value = Mock()
        mock_load_object.return_value = Mock()
        mock_load_transformer.return_value = lambda *args, **kwargs: Mock()
        mock_writer.return_value = Mock()

        # Run pipeline
        run_pipeline("test_config.yaml", dry_run=dry_run)

        # Verify calls
        mock_load_config.assert_called_once()
        mock_logger.assert_called_once()
        mock_read_data.assert_called_once()
        assert mock_load_object.call_count >= 2  # Called for schema and output schema
        mock_load_transformer.assert_called_once()

        if dry_run:
            mock_writer.return_value.write.assert_not_called()
        else:
            mock_writer.assert_called_once()
            mock_writer.return_value.write.assert_called_once()


def test_run_pipeline_config_error():
    with patch("main.load_pipeline_config") as mock_load_config:
        mock_load_config.side_effect = Exception("Config error")

        with pytest.raises(ValueError, match="Error trying to import configuration"):
            run_pipeline("invalid_config.yaml")


def test_cli_run_command(capsys):
    with patch("sys.argv", ["main.py", "run", "--config", "test.toml"]), patch("main.run_pipeline") as mock_run:
        cli()
        mock_run.assert_called_once_with(config="test.toml", save_method="parquet", mode="append", dry_run=False)


# def test_cli_list_command(capsys):
#     with (
#         patch("sys.argv", ["main.py", "list", "--dir"]),
#         patch("pathlib.Path.glob") as mock_glob,
#         patch("pathlib.Path.resolve", return_value="/test/path"),
#     ):
#         # print()
#         mock_glob.return_value = []
#         cli()
#         captured = capsys.readouterr()
#         print(captured.out)
#         # assert "No configuration folder. use --dir to provide a path to configuration files." in captured.out
#         # assert "Available configurations:" in captured.out
#         mock_glob.return_value = [Path("test1.toml"), Path("test2.toml")]
#         cli()
#         captured = capsys.readouterr()
#         assert "Available configurations:" in captured.out
#         assert "/test/path" in captured.out
#         pass


def test_cli_validate_command(capsys):
    with patch("sys.argv", ["main.py", "validate", "--config", "test.toml"]), patch("main.load_pipeline_config") as mock_validate:
        cli()
        mock_validate.assert_called_once()
        captured = capsys.readouterr()
        assert "Successfully validated config file: test.toml" in captured.out


def test_cli_read_command(capsys):
    mock_df = Mock()
    mock_df.head.return_value = "test_data"
    with patch("sys.argv", ["main.py", "read", "--file", "test.csv"]), patch("main.read_input_data", return_value=mock_df):
        cli()
        captured = capsys.readouterr()
        assert "test_data" in captured.out
        assert "Successfully loaded file:" in captured.out


def test_cli_no_command():
    with patch("sys.argv", ["main.py"]), patch("argparse.ArgumentParser.print_help") as mock_help:
        cli()
        mock_help.assert_called_once()


def test_cli_list_no_dir_provided(capsys):
    with patch("sys.argv", ["main.py", "list"]):
        cli()
        captured = capsys.readouterr()
        assert "No configuration folder. use --dir to provide a path to configuration files." in captured.out


def test_cli_list_empty_directory(tmp_path, capsys):
    with patch("sys.argv", ["main.py", "list", "--dir", str(tmp_path)]):
        cli()
        captured = capsys.readouterr()
        assert "No TOML configuration files found." in captured.out


def test_cli_list_with_toml_files(tmp_path, capsys):
    f1 = tmp_path / "one.toml"
    f2 = tmp_path / "two.toml"
    f1.write_text("a")
    f2.write_text("b")

    with patch("sys.argv", ["main.py", "list", "--dir", str(tmp_path)]):
        cli()
        captured = capsys.readouterr()
        out = captured.out
        assert "Available configurations:" in out
        assert str(f1.resolve()) in out
        assert str(f2.resolve()) in out
