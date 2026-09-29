from functools import singledispatch
from typing import List, Callable, TypeVar
import re

T = TypeVar("T")

NUMBER_RE = re.compile(r"\d+(\.\d+)?")


def ID(x: T) -> T:
    return x


def extract_numbers(
    src: str,
    to_number: Callable[[str], T] = ID,  # type: ignore
) -> List[T]:
    """
    Extract all numbers from string.
    """
    return [to_number(m.group(0)) for m in NUMBER_RE.finditer(src)]


@singledispatch
def number(obj) -> float | int:
    raise TypeError(f"invalid type: {type(obj).__name__}")


number.register(int)(lambda x: x)
number.register(str)(lambda x: number(float(x)))
number.register(type(None))(lambda _: 0)


@number.register(float)
def _(x):
    if int(x) == x:
        return int(x)
    return x


def humanize_time(sec: float) -> str:
    """
    Convert seconds to human-readable time.
    """
    if sec < 0.1:
        return f"{1000 * sec:.2f}"[:5] + "ms"
    if sec < 100.0:
        return f"{sec:.2f}"[:5] + "s"
    return f"{sec / 60:.2f}"[:5] + "min"
