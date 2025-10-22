# data-loader
A package that loads python dataframes, applies validation and formatting using pandera models and saves to database

Run the full pipeline
python -m data_pipeline run --config config.toml

Dry-run (validate and transform only)
python -m data_pipeline run --config config.toml --dry-run

Use a custom log file
python -m data_pipeline run --config config.toml --override-log logs/test_run.log

List available pipelines
python -m data_pipeline list --dir configs/