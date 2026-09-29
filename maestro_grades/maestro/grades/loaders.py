from collections import defaultdict
from pathlib import Path
from typing import List, Sequence, Tuple, overload, TYPE_CHECKING
from functools import cmp_to_key

import pandas as pd
import tt

from . import transforms

if TYPE_CHECKING:
    from .models import Course


def read_file(path: Path, **kwargs) -> pd.DataFrame:
    """
    Read file as DataFrame using the default configuration options.
    """
    kwargs.setdefault("index_col", None)
    return tt.read_table(str(path), **kwargs)


@overload
def read_transformed(
    repo: "Course", name: str, path: Path, *, verbose: bool = False
) -> pd.DataFrame:
    ...


@overload
def read_transformed(
    repo: "Course", paths: Sequence[Path], *, verbose: bool = False
) -> pd.DataFrame:
    ...


def read_transformed(*args, verbose=False):
    """
    Read and transform file from data.

    This function has two signatures:

        * read_transformed(course, name, base):
            Read all files in the given base folder having the given name.
        * read_transformed(course, paths):
            Read all files in from the given collection of paths.
    """
    if (n := len(args)) == 3:
        course, name, base = args
        paths = [p for p in base.iterdir() if get_key(p) == name]
    elif n == 2:
        (course, paths) = args
    else:
        raise TypeError("function must be called with 2 or 3 positional arguments")

    source_path, transforms = filter_paths(paths)
    data = read_file(source_path)
    for script in transforms:
        if verbose:
            print(f"transforming by: {script}")
        data = course.transform_by(data, script)
    if verbose and not transforms:
        print("no transformations applied")
    return data


def filter_paths(paths: Sequence[Path]) -> Tuple[Path, List[Path]]:
    """
    Receive a sequence of paths e select the single data source and
    the sequence of transforms to be applied.
    """
    data = []
    transforms = []

    for path in paths:
        ext = path.name.rpartition(".")[-1]
        if ext in ("csv", "xls", "xlsx"):
            data.append(path)
        elif ext in ("py", "tt", "transform"):
            transforms.append(path)

    if not data:
        raise ValueError("no source path found")
    source = max(data, key=cmp_to_key(_cmp_path_priorities))
    transforms = sorted(transforms, key=cmp_to_key(_cmp_path_priorities))
    return source, transforms


def _cmp_path_priorities(p1: str, p2: str) -> int:
    """
    Compare path names p1 and p2 for priority.

    Return -1, if p1 is preferable; 0, if equivalent and +1,
    if p2 is preferable.
    """
    raise NotImplementedError


def file_groups(base: Path) -> List[List[Path]]:
    """
    Iterate over all file names in the given folder.
    """
    groups = defaultdict(list)
    for path in base.iterdir():
        if path.name.startswith("_"):
            continue
        key = get_key(path)
        groups[key].append(path)
    return list(groups.values())


def get_key(path: Path) -> str:
    """
    Return normalized name for the given path.

    The default behavior is to strip extensions.
    """
    return path.name.partition(".")[0]
