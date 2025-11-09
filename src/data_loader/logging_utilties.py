import logging
from pathlib import Path
import datetime


def get_timestamp() -> str:
    """Returns a formatted timestamp string.
    The timestamp format is 'YYYY_MM_DD__HH_MM_SS', where:
    - YYYY: year
    - MM: month
    - DD: day
    - HH: hour (24-hour format)
    - MM: minute
    - SS: second
    Returns:
        str: Current timestamp formatted as 'YYYY_MM_DD__HH_MM_SS'
    Example:
        >>> get_timestamp()
        '2023_12_25__14_30_45'
    """

    return datetime.datetime.now().strftime(format="%Y_%m_%d__%H_%M_%S")


def setup_logger(log_file: Path | None = None, level: int = logging.INFO, name: str = "logger"):
    """Set up a simple structured logger."""
    logger: logging.Logger = logging.getLogger(name)
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
