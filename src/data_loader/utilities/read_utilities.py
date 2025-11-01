from pathlib import Path


def validate_path(path: str | Path) -> Path:
    if Path(path).exists():
        return Path(path)

    raise NotADirectoryError(f"Folder does not exist: {path}")


def validate_file(path: str | Path) -> Path:
    if Path(path).exists():
        return Path(path)

    raise FileNotFoundError(f"File does not exist: {path}")
