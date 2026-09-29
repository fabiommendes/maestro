from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Literal, Union, cast, overload, TypeVar, TYPE_CHECKING
from dataclasses import dataclass

Lang = Literal["python", "cpp", "java", "c", "javascript"]
LangLike = Lang | "CodeTransform"
Text = Literal["markdown", "text"]
Fmt = Union[Lang, Text]
T = TypeVar("T")
F = TypeVar("F", Lang, Text)

if TYPE_CHECKING:
    from .code import CodeTransform
    from .runner import Runner

__all__ = [
    "Lang",
    "Text",
    "Fmt",
    "Info",
    "info",
    "format_from_path",
    "code_transform",
    "path_runner",
    "source_runner",
]


@dataclass
class Info:
    name: str
    aliases: List[str]
    extension: str
    description: str = ""


def normalize_format(fmt: str) -> Fmt:
    """
    Normalize language name, e.g. "py" -> "python".
    """
    fmt = fmt.lower()
    try:
        return ALIAS_TO_FMT[fmt]
    except KeyError:
        raise ValueError(f"Unknown language: {fmt!r}")


def format_from_path(path: Path) -> Fmt:
    """
    Get format from path.
    """
    return _format_from_extension(path.suffix)


def _format_from_extension(ext: str) -> Fmt:
    ext = ext.removeprefix(".")
    try:
        return EXTENSIONS_FMT[ext]
    except KeyError:
        raise ValueError(f"Unknown extension: {ext!r}")


def info(fmt: Fmt) -> Info:
    """
    Get format from file extension.
    """
    fmt = normalize_format(fmt)
    try:
        return INFO[fmt]
    except KeyError:
        raise ValueError(f"Unknown format: {fmt!r}")


@overload
def code_transform(fmt: "CodeTransform[T]") -> "CodeTransform[T]":
    ...


@overload
def code_transform(fmt: Fmt) -> "CodeTransform":
    ...


def code_transform(fmt):
    """
    Get code transform for a language.
    """
    from .code import CodeTransform, get_transform_class

    if isinstance(fmt, CodeTransform):
        return fmt

    fmt = normalize_format(fmt)
    return get_transform_class(fmt)()


def path_runner(path: Path, lang: LangLike, **kwargs) -> "Runner":
    """
    Get runner instance that executes code in the given path.
    """
    from .runner import PythonScriptRunner

    code = code_transform(lang)
    match code.lang:
        case "python":
            return PythonScriptRunner(path, **kwargs)
        case _:
            raise NotImplementedError('"{lang}" is not supported yet.')


def source_runner(src: str, lang: LangLike, **kwargs) -> "Runner":
    """
    Get runner instance that executes code from the source file.
    """
    from .runner import PythonSrcScriptRunner

    code = code_transform(lang)
    match code.lang:
        case "python":
            return PythonSrcScriptRunner(src, **kwargs)
        case _:
            raise NotImplementedError('"{lang}" is not supported yet.')


# =============================================================================
#                           Supported languages
# =============================================================================
INFO: Dict[Fmt, Info] = {
    "python": Info(
        name="Python",
        aliases=["py"],
        extension="py",
        description="A dynamic programming language, with batteries included.",
    ),
    "cpp": Info(
        name="C++",
        aliases=["c++", "cpp"],
        extension="cpp",
        description="A system programming language.",
    ),
    "java": Info(
        name="Java",
        aliases=["java"],
        extension="java",
        description="A system programming language.",
    ),
    "c": Info(
        name="C",
        aliases=["c"],
        extension="c",
        description="A system programming language.",
    ),
}

ALIAS_TO_FMT: Dict[str, Fmt] = {k: k for k in INFO}
FMT_EXTENSIONS: Dict[Fmt, str] = {}
EXTENSIONS_FMT: Dict[str, Fmt] = {}


def _init():
    for fmt, info in INFO.items():
        ALIAS_TO_FMT.update((alias, fmt) for alias in info.aliases)
        FMT_EXTENSIONS[fmt] = info.extension
        EXTENSIONS_FMT.setdefault(info.extension, fmt)


_init()

# Enable caches
_format_from_extension = lru_cache(512)(_format_from_extension)  # type: ignore
normalize_format = lru_cache(512)(normalize_format)  # type: ignore
