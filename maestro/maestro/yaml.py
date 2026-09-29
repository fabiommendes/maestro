import io
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML  # type: ignore[import-untyped]

yaml = YAML(typ="rt")
yaml.indent = 2
yaml.default_flow_style = False
yaml.encoding = "utf-8"

load = yaml.load
dump = yaml.dump


def dumps(data: Any) -> str:
    """
    Serialize data to a YAML formatted string.
    """
    fd = io.StringIO()
    yaml.dump(data, fd)
    return fd.getvalue()


def push_data(path: Path, data: dict[str, str]) -> None:
    """
    Push data to the specified path as a YAML file.
    """
    if path.is_dir():
        raise IsADirectoryError(f"cannot save data to a directory: {path}")
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        data = {**yaml.load(path), **data}
    yaml.dump(data, path)
