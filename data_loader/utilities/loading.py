import pandas as pd
from pathlib import Path
import csv
# import struct


class UnsupportedFileTypeError(Exception):
    """Raised when the file type is not recognized or supported."""

    pass


def detect_file_type(file_path: Path) -> str:
    """
    Detect the file type using extension and content inspection.

    Returns
    -------
    str : One of 'csv', 'tsv', 'excel', 'parquet'
    """

    ext = file_path.suffix.lower()

    # 1️⃣ Extension-based hints
    if ext in [".csv"]:
        return "csv"
    if ext in [".tsv"]:
        return "tsv"
    if ext in [".xls", ".xlsx"]:
        return "excel"
    if ext == ".parquet":
        return "parquet"

    # 2️⃣ Content-based detection
    with open(file_path, "rb") as f:
        header = f.read(8)

        # Parquet magic bytes
        if header.startswith(b"PAR1"):
            return "parquet"

        # Excel (XLSX = ZIP file, starts with PK)
        if header.startswith(b"PK"):
            return "excel"

        # Try text-based heuristics
        try:
            text_sample = header.decode("utf-8", errors="ignore")
            if "," in text_sample:
                return "csv"
            if "\t" in text_sample:
                return "tsv"
        except UnicodeDecodeError:
            pass

    # 3️⃣ Fallback heuristic (try CSV sniff)
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(2048)
            dialect = csv.Sniffer().sniff(sample)
            delim = dialect.delimiter
            return "tsv" if delim == "\t" else "csv"
    except Exception:
        pass

    raise UnsupportedFileTypeError(f"Could not determine file type for: {file_path}")


def load_table(file_path: str | Path) -> pd.DataFrame:
    """
    Load a tabular file (CSV, TSV, Excel, or Parquet) into a pandas DataFrame.
    File type is detected automatically from both file extension and content.

    Parameters
    ----------
    file_path : str | Path
        Path to the file.

    Returns
    -------
    pd.DataFrame
    """

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    file_type = detect_file_type(path)

    if file_type == "csv":
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                sample = f.read(2048)
                f.seek(0)
                dialect = csv.Sniffer().sniff(sample)
                return pd.read_csv(f, sep=dialect.delimiter)
        except Exception:
            return pd.read_csv(path)

    elif file_type == "tsv":
        return pd.read_csv(path, sep="\t")

    elif file_type == "excel":
        return pd.read_excel(path)

    elif file_type == "parquet":
        return pd.read_parquet(path)

    else:
        raise UnsupportedFileTypeError(f"Unsupported or unrecognized file type: {file_type}")
