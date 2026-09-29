from __future__ import annotations

from io import IOBase
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Literal,
    Self,
    TextIO,
    TypedDict,
    Unpack,
    overload,
)

from pydantic import BaseModel
from pydantic.main import IncEx
from pydantic_yaml import (
    parse_yaml_file_as,
    parse_yaml_raw_as,
    to_yaml_file,
    to_yaml_str,
)
from ruamel.yaml import YAML

__all__ = [
    "Model",
    "yaml",
    "parse_yaml",
    "dump_yaml",
    "parse_model",
    "dump_model",
]

yaml = YAML(typ="safe")
yaml.indent = 2
yaml.default_flow_style = False
yaml.block_seq_indent = 2
yaml.encoding = "utf-8"


class Model(BaseModel):
    """
    A base model for Pydantic that uses YAML for serialization and
    deserialization.
    """

    CONFIG_FILE: ClassVar[str] = "config.yaml"

    class Config:
        """
        Pydantic configuration to use YAML for serialization.
        """

        json_encoders = {Path: str}
        arbitrary_types_allowed = True

    @classmethod
    def from_file(cls, path: Path) -> Self:
        """
        Load configuration from a file.
        """
        if not path.exists():
            raise FileNotFoundError(f"Configuration file {path} does not exist.")

        data = yaml.load(path.read_text(encoding="utf8"))
        if not isinstance(data, dict):
            msg = f"Expected a dictionary in {path}, got {type(data)} instead."
            raise ValueError(msg)

        if "path" in cls.model_fields:
            data.setdefault("path", path)

        return cls.model_validate(data)

    def model_dump_yaml(self, **kwargs: Unpack[ModelDumpKwargs]) -> str:
        """
        Dump the model to a YAML string.
        """
        return dump_model(self, **kwargs)

    def save(self):
        """
        Save the model to a YAML file.
        """
        path = self._yaml_path()
        yaml = dump_model(self)

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf8") as fd:
            fd.write(yaml)

    def exists(self) -> bool:
        """
        Check if the model's YAML file exists.
        """
        return self._yaml_path().exists()

    def _path(self) -> Path:
        """
        Get the path where the model is saved.
        """
        path = getattr(self, "path", None)
        if path is None:
            msg = "Model must have a 'path' attribute of type Path."
            raise ValueError(msg)
        if not isinstance(path, Path):
            msg = f"Expected 'path' to be a Path, got {type(path)} instead."
            raise TypeError(msg)
        return path

    def _yaml_path(self) -> Path:
        """
        Get the YAML path where the model is saved.
        """
        path = self._path()
        if path.suffix != ".yaml":
            return path / self.CONFIG_FILE
        return path


def parse_yaml(src: str | Path | TextIO) -> Any:
    """
    Parse a YAML file or string into a dictionary.

    Args:
        src: The source YAML file path, string, or TextIO object.

    Returns:
        A dictionary representation of the YAML content.
    """
    if isinstance(src, Path):
        src = src.read_text(encoding="utf8")
    elif isinstance(src, TextIO):
        src = src.read()
    return yaml.load(src)


@overload
def dump_yaml(obj: Any) -> str: ...


@overload
def dump_yaml(obj: Any, to: Path | IOBase): ...


def dump_yaml(obj, to=None):
    """
    Dump a dictionary or Pydantic model to a YAML string or file.

    Args:
        obj: The object to dump.
        to: Optional file path or IOBase object to write the YAML content to.

    Returns:
        A YAML string if `to` is None, otherwise returns None.
    """
    if to is None:
        return yaml.dump(obj)
    elif isinstance(to, Path):
        with to.open("w", encoding="utf8") as fd:
            yaml.dump(obj, fd)
    else:
        yaml.dump(obj, to)


def parse_model(model: type[BaseModel], src: str | Path | TextIO) -> Any:
    """
    Parse a YAML file into a Pydantic model.

    Args:
        model: The Pydantic model to parse into.
    """

    if isinstance(src, Path):
        return parse_yaml_file_as(model, src)
    elif isinstance(src, TextIO):
        src = src.read()
    return parse_yaml_raw_as(model, src)


@overload
def dump_model(model: BaseModel, **kwargs: Unpack[ModelDumpKwargs]) -> str: ...


@overload
def dump_model(
    model: BaseModel, to: Path | IOBase, **kwargs: Unpack[ModelDumpKwargs]
) -> None: ...


def dump_model(model, to=None, **kwargs):
    """
    Dump a Pydantic model to a YAML file.

    Args:
        model: The Pydantic model to dump.
    """
    if to is None:
        return to_yaml_str(model, custom_yaml_writer=yaml, **kwargs)
    elif isinstance(to, Path):
        with to.open("w", encoding="utf8") as fd:
            to_yaml_file(fd, model, custom_yaml_writer=yaml, **kwargs)
    else:
        to_yaml_file(fd, model, custom_yaml_writer=yaml, **kwargs)


class ModelDumpKwargs(TypedDict, total=False):
    indent: int | None
    include: IncEx | None
    exclude: IncEx | None
    context: Any | None
    by_alias: bool | None
    exclude_unset: bool
    exclude_defaults: bool
    exclude_none: bool
    round_trip: bool
    warnings: bool | Literal["none", "warn", "error"]
    fallback: Callable[[Any], Any] | None
    serialize_as_any: bool
