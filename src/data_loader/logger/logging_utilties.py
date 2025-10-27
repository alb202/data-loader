import logging
from pathlib import Path
import datetime


def get_timestamp() -> str:
    return datetime.datetime.now().strftime(format="%Y_%m_%d__%H_%M_%S")


def setup_logger(log_file: str | None = None, level=logging.INFO):
    """Set up a simple structured logger."""
    logger = logging.getLogger("data_pipeline")
    logger.setLevel(level)

    # Avoid duplicate handlers in re-runs
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
