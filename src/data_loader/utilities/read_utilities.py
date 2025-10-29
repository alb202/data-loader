from pathlib import WindowsPath, Path


def validate_path(path: str | WindowsPath) -> WindowsPath:
    if Path(path).exists():
        return Path(path)

    raise NotADirectoryError(f"Folder does not exist: {path}")


def validate_file(path: str | WindowsPath) -> WindowsPath:
    if Path(path).exists():
        return Path(path)

    raise FileNotFoundError(f"File does not exist: {path}")
