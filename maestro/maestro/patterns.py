"""
Specify patterns that declare groups of files and directories.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, ClassVar, Iterable

from pydantic import BeforeValidator, PlainSerializer


def pattern(data: str | Path) -> Pattern:
    """
    Create a pattern from a string or Path.

    Args:
        data:
            A string or Path representing the pattern. If a Path is given, it
            must be an existing file or directory.
    """
    if isinstance(data, str):
        return BasePattern.from_string(data)
    elif isinstance(data, Path):
        if data.is_dir():
            return Dir(data)
        elif data.is_file():
            return File(data)
        else:
            raise ValueError(f"Path {data} is neither a file nor a directory.")
    else:
        raise TypeError(f"Invalid type for pattern: {type(data)}")


class BasePattern(ABC):
    """
    Base class for file patterns.
    """

    is_dir: ClassVar[bool] = False
    is_file: ClassVar[bool] = False
    is_glob: ClassVar[bool] = False

    @classmethod
    def from_string(cls, data: str):
        if "*" in data or "?" in data or "[" in data or "]" in data:
            return Glob(data)
        elif data.endswith("/"):
            return Dir(Path(data))
        else:
            return File(Path(data))

    def to_string(self) -> str:
        """
        Convert the pattern to a string representation.
        """
        match self:
            case File(data):
                return str(data)
            case Dir(data):
                return str(data) + "/"
            case Glob(data):
                return data
            case _:
                raise

    def paths(self, base: Path) -> Iterable[Path]:
        """
        Iterate over the paths that match this pattern in the given base directory.
        """
        match self:
            case File(data):
                yield base / data
            case Dir(data):
                yield base / data
                yield from (base / data).glob("*")
            case Glob(data):
                yield from base.glob(data)


type Pattern = Annotated[
    BasePattern,
    BeforeValidator(lambda x: BasePattern.from_string(x) if isinstance(x, str) else x),
    PlainSerializer(BasePattern.to_string),
]


@dataclass(frozen=True)
class File(BasePattern):
    """
    Represents a file.
    """

    data: Path
    is_file: ClassVar[bool] = True


@dataclass(frozen=True)
class Dir(BasePattern):
    """
    Represents a folder.
    """

    data: Path
    is_dir: ClassVar[bool] = True


@dataclass(frozen=True)
class Glob(BasePattern):
    """
    A glob pattern for matching files.
    """

    data: str
    is_glob: ClassVar[bool] = True


def keep_files(repo: Path, keep: Iterable[Pattern]) -> set[Path]:
    """
    Return a set of paths that should be kept in the repository.

    This is useful for ensuring that certain files are not excluded.
    """
    keep_files = set()
    for pattern in keep:
        match pattern:
            case Dir(data):
                keep_files.add(repo / data)
            case Glob(data):
                keep_files.update(repo.glob(data))
            case File(data):
                keep_files.add(repo / data)
            case _:
                raise TypeError(f"Invalid pattern type: {type(pattern)}")
    return keep_files
