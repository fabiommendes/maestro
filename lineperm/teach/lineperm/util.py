from pathlib import Path
from typing import TypeVar
from teach.run import CodeTransform, format_from_path, code_transform as _code_transform

CT = TypeVar("CT", bound=CodeTransform[str])


def code_transform(obj: str | Path | CodeTransform[str]) -> CodeTransform[str]:
    if isinstance(obj, Path):
        obj = format_from_path(obj)
    if isinstance(obj, CodeTransform):
        return obj
    return _code_transform(obj)  # type: ignore
