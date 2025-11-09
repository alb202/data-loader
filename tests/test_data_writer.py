from data_loader.data_writer import write_parquet, write_duckdb, write_sqlite, write_csv, write_tsv

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import pytest


def test_write_parquet_with_partitions_calls_to_parquet_with_partition_cols(tmp_path: Path):
    # Arrange
    db = "mydb"
    table_name = "my_table"
    partition_cols = ["year", "month"]
    output_base = tmp_path / "out"

    mock_df = Mock()
    writer = SimpleNamespace(
        df=mock_df,
        partition_cols=partition_cols,
        db=db,
        table_name=table_name,
        output_path=output_base,
    )

    expected_path = output_base / db / table_name

    # Act
    write_parquet(writer)

    # Assert
    mock_df.to_parquet.assert_called_once_with(expected_path, partition_cols=partition_cols, index=False)


def test_write_parquet_no_partitions_appends_parquet_extension_and_calls_to_parquet(tmp_path: Path):
    # Arrange
    db = "anotherdb"
    table_name = "events"
    partition_cols = []  # no partitions
    output_base = tmp_path / "out2"

    mock_df = Mock()
    writer = SimpleNamespace(
        df=mock_df,
        partition_cols=partition_cols,
        db=db,
        table_name=table_name,
        output_path=output_base,
    )

    expected_path = output_base / db / f"{table_name}.parquet"

    # Act
    write_parquet(writer)

    # Assert
    mock_df.to_parquet.assert_called_once_with(expected_path, partition_cols=partition_cols, index=False)

    def test_write_duckdb_raises_when_no_table_name(tmp_path: Path):
        writer = SimpleNamespace(
            df=Mock(),
            output_path=tmp_path / "out",
            table_name="",  # missing
            mode="overwrite",
            db="mydb",
        )
        with pytest.raises(ValueError):
            write_duckdb(writer)


def test_write_duckdb_overwrite_calls_drop_create_and_register_and_closes(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out"
    db = "mydb"
    table_name = "tbl"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="overwrite",
        db=db,
    )

    mock_conn = Mock()
    with patch("data_loader.data_writer.duckdb.connect", return_value=mock_conn) as mock_connect:
        write_duckdb(writer)

    expected_path = output_base / (db + ".duckdb")
    mock_connect.assert_called_once_with(str(expected_path))

    # register should be called to bind the dataframe
    mock_conn.register.assert_called_once_with("tmp_df", df)

    # DROP TABLE should be called in overwrite mode
    mock_conn.execute.assert_any_call(f"DROP TABLE IF EXISTS {table_name}")
    # CREATE TABLE should be called
    mock_conn.execute.assert_any_call(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_df")
    # INSERT should not be called in overwrite path
    assert all(not (call.args and f"INSERT INTO {table_name}" in call.args[0]) for call in mock_conn.execute.call_args_list)

    mock_conn.close.assert_called_once()


def test_write_duckdb_append_calls_create_and_insert_and_closes(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out2"
    db = "otherdb"
    table_name = "events"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="append",
        db=db,
    )

    mock_conn = Mock()
    with patch("data_loader.data_writer.duckdb.connect", return_value=mock_conn) as mock_connect:
        write_duckdb(writer)

    expected_path = output_base / (db + ".duckdb")
    mock_connect.assert_called_once_with(str(expected_path))

    mock_conn.register.assert_called_once_with("tmp_df", df)

    # DROP should not be called in append mode
    assert all(not (call.args and call.args[0].strip().upper().startswith("DROP TABLE")) for call in mock_conn.execute.call_args_list)

    # CREATE TABLE and INSERT should be called
    mock_conn.execute.assert_any_call(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_df")
    mock_conn.execute.assert_any_call(f"INSERT INTO {table_name} SELECT * FROM tmp_df")

    mock_conn.close.assert_called_once()


def test_write_sqlite_raises_when_no_table_name(tmp_path: Path):
    writer = SimpleNamespace(
        df=Mock(),
        output_path=tmp_path / "out",
        table_name="",  # missing
        mode="overwrite",
        db="mydb",
    )
    with pytest.raises(ValueError):
        write_sqlite(writer)


def test_write_sqlite_overwrite_calls_to_sql_and_closes(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out"
    db = "mydb"
    table_name = "tbl"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="overwrite",
        db=db,
    )

    mock_conn = Mock()
    with patch("data_loader.data_writer.sqlite3.connect", return_value=mock_conn) as mock_connect:
        write_sqlite(writer)

    expected_path = output_base / (db + ".sqlite")
    mock_connect.assert_called_once_with(expected_path)

    df.to_sql.assert_called_once_with(
        name=table_name,
        con=mock_conn,
        if_exists="replace",
        index=False,
    )

    mock_conn.close.assert_called_once()


def test_write_sqlite_append_calls_to_sql_and_closes(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out2"
    db = "otherdb"
    table_name = "events"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="append",
        db=db,
    )

    mock_conn = Mock()
    with patch("data_loader.data_writer.sqlite3.connect", return_value=mock_conn) as mock_connect:
        write_sqlite(writer)

    expected_path = output_base / (db + ".sqlite")
    mock_connect.assert_called_once_with(expected_path)

    df.to_sql.assert_called_once_with(
        name=table_name,
        con=mock_conn,
        if_exists="append",
        index=False,
    )

    mock_conn.close.assert_called_once()


def test_write_tsv_writes_with_header_when_new_file(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out"
    db = "mydb"
    table_name = "tbl"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="overwrite",  # not "append"
        db=db,
    )

    expected_dir = output_base / db
    expected_file = expected_dir / f"{table_name}.tsv"

    # precondition: file should not exist
    assert not expected_file.exists()

    write_tsv(writer)

    df.to_csv.assert_called_once_with(expected_file, sep="\t", mode="w", index=False, header=True)
    # ensure directory was created
    assert expected_dir.exists()


def test_write_tsv_appends_without_header_when_file_exists(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out2"
    db = "otherdb"
    table_name = "events"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="append",
        db=db,
    )

    expected_dir = output_base / db
    expected_file = expected_dir / f"{table_name}.tsv"

    # create existing file to trigger append mode
    expected_dir.mkdir(parents=True, exist_ok=True)
    expected_file.write_text("existing\n")

    write_tsv(writer)

    df.to_csv.assert_called_once_with(expected_file, sep="\t", mode="a", index=False, header=False)
    # file should still exist after write
    assert expected_file.exists()


def test_write_csv_writes_with_header_when_new_file(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out"
    db = "mydb"
    table_name = "tbl"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="overwrite",  # not "append"
        db=db,
    )

    expected_dir = output_base / db
    expected_file = expected_dir / f"{table_name}.csv"

    # precondition: file should not exist
    assert not expected_file.exists()

    write_csv(writer)

    df.to_csv.assert_called_once_with(expected_file, sep=",", mode="w", index=False, header=True)
    # ensure directory was created
    assert expected_dir.exists()


def test_write_csv_appends_without_header_when_file_exists(tmp_path: Path):
    df = Mock()
    output_base = tmp_path / "out2"
    db = "otherdb"
    table_name = "events"
    writer = SimpleNamespace(
        df=df,
        output_path=output_base,
        table_name=table_name,
        mode="append",
        db=db,
    )

    expected_dir = output_base / db
    expected_file = expected_dir / f"{table_name}.csv"

    # create existing file to trigger append mode
    expected_dir.mkdir(parents=True, exist_ok=True)
    expected_file.write_text("existing\n")

    write_csv(writer)

    df.to_csv.assert_called_once_with(expected_file, sep=",", mode="a", index=False, header=False)
    # file should still exist after write
    assert expected_file.exists()
