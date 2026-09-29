"""
This module exports two functions: 

- typecheck(value, type): 
    Verify that value is of the correct type.

- normalize(value, type): 
    Coerce value to the correct type, when no lossless coercion is possible.

"""
from pathlib import Path
from types import GenericAlias, UnionType
from typing import Any, ForwardRef, Union, Type, TypeVar
from functools import singledispatch

TypeHint = Union[type, GenericAlias, UnionType, TypeVar, ForwardRef, str, None]
T = TypeVar("T", bound=type)

ATOMIC_TYPES = int, float, bool, str, bytes, Path, type(None)


class Typechecker:
    """
    Verify that value is of the correct type.

    This works similarly to the isinstance() function, but it also supports
    type hints from the typing module.

    Type checks can be more strict and expensive than isinstance(), like in the example
    >>> typecheck([1, 2, 3.0], list[int])
    False
    """

    def __call__(self, value: Any, cls: TypeHint) -> bool:
        if isinstance(cls, type):
            impl = _typecheck.dispatch(cls)
            return impl(value, cls)
        else:
            impl = _typecheck_special.dispatch(type(cls))
            return impl(value, cls)


typecheck = Typechecker()


# =============================================================================
@singledispatch
def _typecheck(value: Any, cls: type) -> bool:
    """
    Dispatch the type check for the different types of generic functions.
    """
    # Generic aliases are classes, but must be handled as parametrized
    # generic types
    if isinstance(cls, GenericAlias):
        if not isinstance(value, cls.__origin__):  # type: ignore
            return False

        impl = _typecheck_generic.dispatch(cls.__origin__)
        if any(isinstance(T, str) for t in cls.__args__):  # type: ignore
            raise TypeError(f"Cannot type generic type with forward references {cls}")
        return impl(value, cls.__origin__, cls.__args__)

    return isinstance(value, cls)


for _T in ATOMIC_TYPES:
    _typecheck.register(_T, isinstance)


# =============================================================================
@singledispatch
def _typecheck_special(value: Any, hint: TypeHint) -> bool:
    """
    Dispatch type hints that are not types.
    """
    raise NotImplementedError(f"{hint} -> {type(hint)}")


@_typecheck_special.register(UnionType)
def _(value, hint):
    check = typecheck
    return any(check(value, t) for t in hint.__args__)


@_typecheck_special.register(type(None))
def _(value, _):
    return value is None


@_typecheck_special.register(str)
def _(value, hint):
    raise TypeError(f"cannot typecheck forward references: {hint!r}")


# =============================================================================
@singledispatch
def _typecheck_generic(value: T, cls: Type[T], args: tuple[TypeHint, ...]) -> bool:
    """
    Dispatch generic types.
    """
    args = ", ".join(map(show_hint, args))  # type: ignore
    raise NotImplementedError(f"typecheck({value!r}, {cls.__name__}[{args}])")


@_typecheck_generic.register(dict)
def _(value, _, args):  # type: ignore
    kT, vT = args
    check = typecheck
    return all(check(k, kT) and check(v, vT) for k, v in value.items())


@_typecheck_generic.register(list)
@_typecheck_generic.register(set)
@_typecheck_generic.register(frozenset)
def _(xs, _, args):  # type: ignore
    (eT,) = args
    check = typecheck
    return all(check(x, eT) for x in xs)


# =============================================================================
def show_type(value: Any) -> str:
    """
    Return a string representation of the type of value.
    """
    return type(value).__name__


def show_hint(hint: TypeHint) -> str:
    """
    Return a string representation of the type hint.
    """
    try:
        return hint.__name__  # type: ignore
    except AttributeError:
        return str(hint)
