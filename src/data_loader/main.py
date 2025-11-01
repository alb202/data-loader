from pathlib import Path
# import pandera as pa

# from extract.csv_extractor import CSVExtractor
# from transform.example_transform import ExampleTransform
# from load.parquet_loader import ParquetLoader
# from data_loader.extract.extract_base import Extract
# from data_loader.transform.example_transform import Extract
# from data_loader.load.load_parquet import ParquetLoader
from logger.logging_utilties import setup_logger, get_timestamp
from config.pipeline_config_io import load_pipeline_config  # , #load_config
from extract.read_data import read_input_data
from utilities.object_loader import load_object_from_file
from models.extract_pipeline_data_model import ExtractPipelineData
from utilities.class_loader import load_class_from_file  # DynamicClassLoader, load_class,
from load.writer import DataFrameWriter  # , DataFrameWriterRegistry

# from src.data_loader.
import argparse
# from pathlib import Path
# import glob
# import pandas as pd


SAVE_METHODS = ["parquet", "duckdb", "sqlite"]


def run_pipeline(
    config: str,
    # input_paths,
    # output_dest: str,
    # extractor_cls,
    # transformer_cls,
    # loader_cls,
    # logger,
    # input_schema=None,
    # output_schema=None,
    dry_run=False,
    save_method: str = "parquet",
) -> None:
    # Handle single or multiple input files
    # input_pattern = config["input_path"]
    # input_paths = sorted(glob.glob(input_pattern))
    # if not input_paths:
    #     logger.error(f"No input files found for pattern: {input_pattern}")
    #     return

    # log_file = config["name"] + ".log"

    logger = setup_logger(log_file=Path("../../logs/").resolve() / f"log_file__{get_timestamp()}")

    try:
        config_dict = load_pipeline_config(config)
        logger.info("Configuration file successfully loaded.")
        logger.info(config_dict)
    except Exception as e:
        print(e)
        msg = "Error trying to import configuration. Check format and try again."
        logger.exception(msg)
        raise ValueError(msg)

    logger.info(f"Pipeline name: {config_dict.details.name}")
    logger.info(f"Pipeline description: {config_dict.details.description}")
    logger.info(f"Dry-run mode: {dry_run}")

    # Extract

    extract_files: list = []
    for file_number, extract_file in enumerate(config_dict.extract_files):
        data = read_input_data(folder=extract_file.folder, file_name=extract_file.file_name)
        logger.info(f"File {file_number}: {extract_file.file_name} has been loaded")

        schema = load_object_from_file(
            folder_name=Path("sample_models/").resolve(), file_name=extract_file.schema_name + ".py", object_name="schema"
        )
        logger.info(f"Schema {file_number}: {extract_file.schema_name} has been loaded")

        validated_data = schema.validate(data)
        logger.info(f"Data '{extract_file.label}' has been validated")

        extract_files.append(ExtractPipelineData(label=extract_file.label, schema=schema, data=validated_data))

    print(extract_files)

    # Transform

    print("transformer_pipeline", "src/data_loader/transform/" + config_dict.details.transformer_pipeline + ".py")

    load_schema_from_file: dict = dict(
        filepath="sample_models/" + config_dict.output_table.schema_name + ".py",
        class_name="schema",
        package_root="/Users/ab/Projects/data-loader/",
    )
    load_transformer_from_file: dict = dict(
        filepath="src/data_loader/transform/" + config_dict.details.transformer_pipeline + ".py",
        class_name="Transformer",
        package_root="/Users/ab/Projects/data-loader/",
    )

    print("load_schema_from_file", load_schema_from_file)
    print("load_transformer_from_file", load_transformer_from_file)

    output_schema = load_class_from_file(**load_schema_from_file)
    transformer_class = load_class_from_file(**load_transformer_from_file)
    #     folder_path="src/data_loader/transform/", file_name=config_dict.details.transformer_pipeline, class_name="transform"
    # )
    # transformer_class.transform()

    # transformer_class = DynamicClassLoader(
    #     folder_path="src/data_loader/transform/", file_name=config_dict.details.transformer_pipeline, class_name="transform"
    # )
    transformed_df = transformer_class.transform(*[extract_file.data for extract_file in extract_files], schema=output_schema)
    print(transformed_df)

    # Load

    logger.info(f"Saving data to disk with method: {save_method}")
    logger.info(f"Output location: {'sample_output/'}")
    logger.info(f"Database: {config_dict.output_table.db}")
    logger.info(f"Table name: {config_dict.output_table.table_name}")

    if not dry_run:
        DataFrameWriter(
            df=transformed_df.assign(data_label=config_dict.output_table.data_label),
            output_path=Path("sample_output/"),
            write_method=save_method,
            table_name=config_dict.output_table.table_name,
            db=config_dict.output_table.db,
            mode="overwrite",
            validate=False,
            partition_cols=["data_label"],
            schema=None,
        ).write()
    else:
        logger.info("Dry-run selected. No data written.")
    logger.info("Pipeline execution complete!")

    # """Core ETL pipeline execution logic."""
    # transformer = transformer_cls()
    # loader = loader_cls()
    # all_results = []

    # for input_path in input_paths:
    #     extractor = extractor_cls()
    #     logger.info(f"Extracting data from: {input_path}")
    #     df = extractor.read(Path(input_path))
    #     logger.info(f"→ Loaded {len(df)} rows from {Path(input_path).name}")

    #     # Validate input
    #     if input_schema:
    #         logger.info("Validating input schema...")
    #         df = transformer.validate_input(df, input_schema)
    #         logger.info("Input validation passed")

    #     # Transform
    #     df = transformer.transform(df)
    #     logger.info("Transformation complete")

    #     # Validate output
    #     if output_schema:
    #         logger.info("Validating output schema...")
    #         df = transformer.validate_output(df, output_schema)
    #         logger.info("Output validation passed")

    #     if dry_run:
    #         logger.info("Dry-run enabled: skipping data output.")
    #     else:
    #         loader.write(df, output_dest)
    #         logger.info(f"Data written to {output_dest}")

    #     all_results.append(df)

    # combined = pd.concat(all_results, ignore_index=True)
    # logger.info(f"Combined total rows processed: {len(combined)}")
    # return combined


def cli():
    """Command-line interface for data pipeline execution."""
    parser = argparse.ArgumentParser(description="Data Pipeline CLI - Run ETL pipelines using TOML configs")

    subparsers = parser.add_subparsers(dest="command", help="Subcommands")

    # run command
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("--config", required=True, help="Path to the TOML config")
    run_parser.add_argument("--save_method", required=False, default="parquet", choices=SAVE_METHODS, help="Method for saving data")
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run validation and transformation only, skip loading",
    )
    # run_parser.add_argument(
    #     "--override-log",
    #     help="Optional log file path override",
    # )

    # list command
    list_parser = subparsers.add_parser("list", help="List available TOML configs")
    list_parser.add_argument("--dir", required=False, default="sample_configs/", help="Directory to search for .toml files")

    args = parser.parse_args()

    if args.command == "run":
        # config_path = Path(args.config)
        # if not config_path.exists():
        #     print(f"Config file not found: {config_path}")
        #     return

        # config = load_config(config_path)
        # log_file = args.override_log or config["pipeline_name"] + ".log"

        # logger = setup_logger(
        #     log_file=config.get("log_file", log_file),
        # )

        # logger.info(f"Starting pipeline: {config['pipeline_name']}")
        # logger.info(f"Dry-run mode: {args.dry_run}")

        # # Handle single or multiple input files
        # input_pattern = config["input_path"]
        # input_paths = sorted(glob.glob(input_pattern))
        # if not input_paths:
        #     logger.error(f"No input files found for pattern: {input_pattern}")
        #     return

        run_pipeline(
            config=args.config,
            save_method=args.save_method,
            # output_dest=config["output_dest"],
            # extractor_cls=config["extractor_cls"],
            # transformer_cls=config["transformer_cls"],
            # loader_cls=config["loader_cls"],
            # logger=logger,
            # input_schema=config["input_schema"],
            # output_schema=config["output_schema"],
            dry_run=args.dry_run,
        )

        # logger.info("Pipeline execution complete!")

    elif args.command == "list":
        config_dir = Path(args.dir).resolve()
        toml_files = list(config_dir.glob("*.toml"))
        if not toml_files:
            print("No TOML configuration files found.")
        else:
            print("Available configurations:")
            for f in toml_files:
                print(f"  - {f.resolve()}")

    else:
        parser.print_help()


if __name__ == "__main__":
    cli()
