"""
String utility functions.

Sometimes we import from other libs, sometimes we implement it here.
"""

from functools import lru_cache
from typing import Tuple, Union, TypeVar
import re

INDENTATION = re.compile(r"^\s+")
T = TypeVar("T")


def clean_indentation(line: str, tabs: Union[str, int] = 4) -> Tuple[int, str]:
    """
    Return the pair of (indentation level, line) with the line
    stripped from the original indentation.

    Example:
        >>> clean_indentation("    foo")
        (4, "foo")
    """

    if isinstance(tabs, int):
        tabs = " " * tabs

    if (m := INDENTATION.match(line)) is None:
        return 0, line

    prefix = m.group(0)
    clean = line[len(prefix) :]
    prefix = prefix.replace("\t", tabs)
    return (len(prefix), clean)


def split_indentation(src: str) -> Tuple[str, str]:
    """
    Split the indentation from the rest of the line.

    Example:
        >>> split_indentation("    foo")
        ("    ", "foo")
    """
    if (m := INDENTATION.match(src)) is None:
        return "", src
    return m.group(0), src[len(m.group(0)) :]


@lru_cache(256)
def clean_string(st: str) -> str:
    """
    Strip all whitespace from string ends.

    Example:
        >>> clean_string("  foo  ")
        "foo"
    """
    return st.strip()


def indent(st: str, n: int | str = "    ") -> str:
    """
    Indent string by the given indentation level or indentation string.

    Example:
        >>> indent("foo")
        "    foo"
    """
    indent = " " * n if isinstance(n, int) else n
    return "".join(indent + ln for ln in st.splitlines(keepends=True))


def humanize(st: str) -> str:
    """
    Humanize string replacing underscores by spaces.

    Example:
        >>> humanize("foo_bar")
        "foo bar"
    """
    return st.replace("_", " ").replace("-", " ")
