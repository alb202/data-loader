from logger.logging_utilties import setup_logger, get_timestamp
from config.pipeline_config_io import load_pipeline_config  # , #load_config
from extract.read_data import read_input_data
from utilities.object_loader import load_object_from_file
from models.extract_pipeline_data_model import ExtractPipelineData
from utilities.transformer_loader import load_transformer_function
from data_writer.writer import DataFrameWriter

import argparse
from pathlib import Path


SAVE_METHODS = ["parquet", "duckdb", "sqlite", "tsv", "csv"]
MODES = ["append", "overwrite"]
DEFAULT_PATHS = {
    "signature_model": "src/models/default_signature_model.py",
}


def run_pipeline(
    config: str,
    mode: str = "append",
    dry_run: bool = False,
    save_method: str = "parquet",
) -> None:
    try:
        config_dict = load_pipeline_config(path=Path(config))
        # logger.info("Configuration file successfully loaded.")
        # logger.info(config_dict)
    except Exception as e:
        print(e)
        # logger.exception(msg)
        raise ValueError("Error trying to import configuration. Check format and try again.")

    logger = setup_logger(
        log_file=Path(config_dict.details.project_path).resolve() / "logs" / f"log_file__{get_timestamp()}", name="Logger"
    )
    logger.info("Logging started")

    logger.info(f"Pipeline name: {config_dict.details.name}")
    logger.info(f"Pipeline description: {config_dict.details.description}")
    logger.info(f"Dry-run mode: {dry_run}")

    # Extract
    extract_files: list = []
    for file_number, extract_file in enumerate(config_dict.extract_files):
        data = read_input_data(path=extract_file.data_file)
        logger.info(f"File {file_number}: {extract_file.data_file} has been loaded")

        schema = load_object_from_file(
            folder_name=Path(config_dict.details.project_path).resolve(), file_name=extract_file.schema_file + ".py", object_name="schema"
        )
        logger.info(f"Schema {file_number}: {extract_file.schema_file} has been loaded")

        validated_data = schema.validate(data)
        logger.info(f"Data '{extract_file.label}' has been validated")

        extract_files.append(ExtractPipelineData(label=extract_file.label, schema=schema, data=validated_data))

    # Transform
    output_schema = load_object_from_file(
        folder_name=Path(config_dict.output_table.schema_file).parent.resolve(),
        file_name=config_dict.output_table.schema_file + ".py",
        object_name="schema",
    )

    func = load_transformer_function(
        transformer_file=Path(config_dict.details.project_path) / (config_dict.details.transformer_pipeline + ".py"),
        template_file=Path(DEFAULT_PATHS.get("signature_model")).resolve(),
    )

    transformed_df = func(*[extract_file.data for extract_file in extract_files], output_schema=output_schema)
    transformed_df = output_schema.validate(transformed_df)

    # transformer_class = load_object_from_file(
    #     folder_name=Path(config_dict.details.project_path).resolve(),
    #     file_name=config_dict.details.transformer_pipeline + ".py",
    #     object_name="Transformer",
    # )

    # transformed_df = transformer_class.transform(*[extract_file.data for extract_file in extract_files], output_schema=output_schema)
    # transformer_class.validate_output(df=transformed_df, output_schema=output_schema)

    # Load
    logger.info(f"Saving data to disk with method: {save_method}")
    logger.info(f"Output location: {config_dict.output_table.output_path}")
    logger.info(f"Database: {config_dict.output_table.db}")
    logger.info(f"Table name: {config_dict.output_table.table_name}")

    if not dry_run:
        DataFrameWriter(
            df=transformed_df.assign(data_label=config_dict.output_table.data_label),
            output_path=Path(config_dict.output_table.output_path).resolve(),
            write_method=save_method,
            table_name=config_dict.output_table.table_name,
            db=config_dict.output_table.db,
            mode=mode,
            partition_cols=["data_label"],
        ).write()
    else:
        logger.info("Dry-run selected. No data written.")
    logger.info("Pipeline execution complete!")


def cli():
    """Command-line interface for data pipeline execution."""
    parser = argparse.ArgumentParser(description="Data Pipeline CLI - Run ETL pipelines using TOML configs")

    subparsers = parser.add_subparsers(dest="command", help="Subcommands")

    # run command
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("--config", required=True, help="Path to the TOML config")
    run_parser.add_argument("--save_method", required=False, default="parquet", choices=SAVE_METHODS, help="Method for saving data")
    run_parser.add_argument("--mode", required=False, default="append", choices=MODES, help="Method for saving data")
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run validation and transformation only, skip loading",
    )

    # list command
    list_parser = subparsers.add_parser("list", help="List available TOML configs")
    list_parser.add_argument("--dir", required=False, default=DEFAULT_PATHS.get("configs"), help="Directory to search for .toml files")

    list_parser = subparsers.add_parser("validate", help="Validate the structure of a config file")
    list_parser.add_argument("--config", required=False, default="", help="Configuration file to validate")

    list_parser = subparsers.add_parser("read", help="Test the reading of a data file")
    list_parser.add_argument("--file", required=False, default="", help="Data file to read")

    args = parser.parse_args()

    if args.command == "run":
        run_pipeline(
            config=args.config,
            save_method=args.save_method,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    elif args.command == "list":
        config_dir = Path(args.dir).resolve()
        toml_files = list(config_dir.glob("*.toml"))
        if not toml_files:
            print("No TOML configuration files found.")
        else:
            print("Available configurations:")
            for f in toml_files:
                print(f"  - {f.resolve()}")
    elif args.command == "validate":
        load_pipeline_config(path=Path(args.config))
        print(f"Successfully validated config file: {args.config}")
    elif args.command == "read":
        print(read_input_data(path=Path(args.file)).head())
        print(f"Successfully loaded file: {Path(args.file)}")
    else:
        parser.print_help()


if __name__ == "__main__":
    cli()
