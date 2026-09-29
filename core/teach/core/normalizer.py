from pathlib import Path
from types import GenericAlias, UnionType
from typing import Any, Callable, Union, Type, TypeVar
from functools import singledispatch

from .typechecker import TypeHint, typecheck

T = TypeVar("T", bound=type)

COERCIONS: dict[tuple[type, TypeHint], Callable[[Any], Any]] = {
    (str, bytes): lambda x: x.encode("utf-8"),
    (int, float): float,
    (int, complex): complex,
    (float, complex): complex,
    (str, Path): Path,
    (Path, str): str,
}


class Normalizer:
    """
    Coerce value to the correct type, when no lossless coercion is possible.
    """

    def __call__(self, value: Any, cls: TypeHint) -> Any:
        try:
            if isinstance(cls, cls):  # type: ignore
                return value
        except TypeError:
            pass
        impl = _normalize.dispatch(cls)
        return impl(cls, value)

    def register(self, src: type, dst: type, coerce=None):
        """
        Register a normalization function.
        """
        if coerce is None:
            coerce = dst
            COERCIONS[src, dst] = dst

            # Can be used as a decorator
            return lambda fn: self.register(src, dst, fn) or fn
        else:
            COERCIONS[src, dst] = coerce


normalize = Normalizer()


@singledispatch
def _normalize(cls: TypeHint, value: Any) -> bool:
    """
    Dispatch the type check for the different types of generic functions.
    """
    if typecheck(value, cls):
        return value
    raise NotImplementedError(f"normalize({value!r}, {cls})")


@_normalize.register(type)
def _(cls, value):
    try:
        coerce = COERCIONS[type(value), cls]
    except KeyError:
        raise TypeError(f"cannot coerce {type(value)} to {cls}")
    else:
        return coerce(value)
