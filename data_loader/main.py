from pathlib import Path
import pandera as pa

# from extract.csv_extractor import CSVExtractor
# from transform.example_transform import ExampleTransform
# from load.parquet_loader import ParquetLoader
# from data_loader.extract.extract_base import Extract
# from data_loader.transform.example_transform import Extract
from data_loader.load.load_parquet import ParquetLoader
from data_loader.config.load_config import load_config
from data_loader.logging.logging_utilties import setup_logger

import argparse
from pathlib import Path
import glob
import pandas as pd


def run_pipeline(
    input_paths,
    output_dest: str,
    extractor_cls,
    transformer_cls,
    loader_cls,
    logger,
    input_schema=None,
    output_schema=None,
    dry_run=False,
):
    """Core ETL pipeline execution logic."""
    transformer = transformer_cls()
    loader = loader_cls()
    all_results = []

    for input_path in input_paths:
        extractor = extractor_cls()
        logger.info(f"Extracting data from: {input_path}")
        df = extractor.read(Path(input_path))
        logger.info(f"→ Loaded {len(df)} rows from {Path(input_path).name}")

        # Validate input
        if input_schema:
            logger.info("Validating input schema...")
            df = transformer.validate_input(df, input_schema)
            logger.info("✅ Input validation passed")

        # Transform
        df = transformer.transform(df)
        logger.info("✅ Transformation complete")

        # Validate output
        if output_schema:
            logger.info("Validating output schema...")
            df = transformer.validate_output(df, output_schema)
            logger.info("✅ Output validation passed")

        if dry_run:
            logger.info("Dry-run enabled: skipping data output.")
        else:
            loader.write(df, output_dest)
            logger.info(f"✅ Data written to {output_dest}")

        all_results.append(df)

    combined = pd.concat(all_results, ignore_index=True)
    logger.info(f"📊 Combined total rows processed: {len(combined)}")
    return combined


def cli():
    """Command-line interface for data pipeline execution."""
    parser = argparse.ArgumentParser(description="Data Pipeline CLI - Run ETL pipelines using TOML configs")

    subparsers = parser.add_subparsers(dest="command", help="Subcommands")

    # run command
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("--config", required=True, help="Path to the TOML config")
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run validation and transformation only, skip loading",
    )
    run_parser.add_argument(
        "--override-log",
        help="Optional log file path override",
    )

    # list command
    list_parser = subparsers.add_parser("list", help="List available TOML configs")
    list_parser.add_argument("--dir", default=".", help="Directory to search for .toml files")

    args = parser.parse_args()

    if args.command == "run":
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"❌ Config file not found: {config_path}")
            return

        config = load_config(config_path)
        log_file = args.override_log or config["pipeline_name"] + ".log"

        logger = setup_logger(
            log_file=config.get("log_file", log_file),
        )

        logger.info(f"🚀 Starting pipeline: {config['pipeline_name']}")
        logger.info(f"Dry-run mode: {args.dry_run}")

        # Handle single or multiple input files
        input_pattern = config["input_path"]
        input_paths = sorted(glob.glob(input_pattern))
        if not input_paths:
            logger.error(f"No input files found for pattern: {input_pattern}")
            return

        run_pipeline(
            input_paths=input_paths,
            output_dest=config["output_dest"],
            extractor_cls=config["extractor_cls"],
            transformer_cls=config["transformer_cls"],
            loader_cls=config["loader_cls"],
            logger=logger,
            input_schema=config["input_schema"],
            output_schema=config["output_schema"],
            dry_run=args.dry_run,
        )

        logger.info("🎉 Pipeline execution complete!")

    elif args.command == "list":
        config_dir = Path(args.dir)
        toml_files = list(config_dir.glob("*.toml"))
        if not toml_files:
            print("No TOML configuration files found.")
        else:
            print("📂 Available configurations:")
            for f in toml_files:
                print(f"  - {f}")

    else:
        parser.print_help()


if __name__ == "__main__":
    cli()
