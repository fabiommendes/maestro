import functools
import json
import primitive
import operator
import sys
from collections import defaultdict
from types import GenericAlias, MappingProxyType, UnionType
from copy import deepcopy
from typing import (
    Any,
    Container,
    ForwardRef,
    Iterator,
    Literal,
    ClassVar,
    Protocol,
    TypeVar,
    cast,
    get_type_hints,
    overload,
)
from pathlib import Path
from dataclasses import Field
from weakref import ref

from . import fred
from .error import Error, WrongTypeError
from .typechecker import TypeHint, typecheck
from .normalizer import normalize
from .builder import Kwargs, build

T = TypeVar("T")
M = TypeVar("M", bound="Model")
FieldNames = Container[str]


class Model:
    """
    Base class for dataclass models.
    """

    __dataclass_fields__: ClassVar[dict[str, Field]]
    _model_loaded_type_hints_ = False

    @classmethod
    def load_file(cls: type[M], path: Path, **kwargs) -> M:
        """
        Load object from a file.
        """
        cls.update_type_hints(_auto=True)
        new = cls.fred_loader(path)

        if isinstance(new, dict):
            new.pop("tag", None)
            new.pop("meta", None)
            new = build(new, cls)

        if not isinstance(new, cls):
            raise TypeError(f"path does not contain a {cls.__name__}, got {new}")

        for k, v in kwargs.items():
            if k in cls.__dataclass_fields__ or hasattr(cls, k):
                setattr(new, k, v)
            else:
                raise TypeError(f"invalid attribute: {k}")
        return new

    @classmethod
    def fred_loader(cls, path: Path) -> Any:
        """
        Load a model data from a FRED file.
        """
        return fred.load(path)

    @classmethod
    def update_type_hints(cls, _auto=False, **kwargs):
        """
        Load type hints for this model, resolving forward references.
        """
        if _auto and cls.__dict__.get("_model_loaded_type_hints_", False):
            return

        visited = set()
        globalns = {}
        for base in cls.mro():
            mod = ""
            for part in base.__module__.split("."):
                mod = f"{mod}.{part}".removeprefix(".")
                if mod in visited:
                    continue
                if mod in sys.modules:
                    globalns.update(sys.modules[mod].__dict__)
                visited.add(mod)

        hints = get_type_hints(cls, localns=kwargs or None)
        for name, field in cls.__dataclass_fields__.items():
            typ = hints.get(name, field.type)
            field.type = resolve_type_hint(
                cls, name, typ, globalns=globalns, localns=kwargs, raises=True
            )

        cls._model_loaded_type_hints_ = True

    def normalized_data(self, *, skip=(), fail=None) -> dict[str, Any]:
        """
        Return a dictionary with all attributes cleaned and normalized.

        This process perform (mostly) lossless coercion to the correct
        datatypes.

        Args:
            skip:
                List of attributes to skip during normalization.
            fail:
                If True, call this function on values that fail normalization.
        """
        data = {}

        for attr, field in self.__dataclass_fields__.items():
            if attr in skip:
                continue
            value = getattr(self, attr)

            if not typecheck(value, field.type):
                try:
                    data[attr] = normalize(value, field.type)
                except TypeError:
                    if fail is None:
                        raise
                    else:
                        data[attr] = fail(value)
            else:
                data[attr] = value

        return data

    def copy(self: M) -> M:
        """
        Return a copy of object.
        """
        return deepcopy(self)

    def copy_normalized(self: M) -> M:
        """
        Return a copy of object with normalized data.
        """
        data = self.normalized_data()
        return build(data, type(self))

    @overload
    def validate(
        self, how: Literal["eager"], *, skip: FieldNames = ()
    ) -> tuple[str, Any] | None:
        ...

    @overload
    def validate(
        self, how: Literal["all"], *, skip: FieldNames = ()
    ) -> dict[str, list[Error]]:
        ...

    @overload
    def validate(self, how: Literal["raises"], *, skip: FieldNames = ()) -> None:
        ...

    @overload
    def validate(self, how: Literal["bool"], *, skip: FieldNames = ()) -> bool:
        ...

    def validate(self, how="eager", *, skip=()):
        """
        Validate object.

        Args:
            how:
                How to validate object.
                - eager: Return only first error, if present.
                - all: Return a dictionary mapping fields to list of errors.
                - raises: Raises ValueError with a dictionary if any errors are found.
                - bool: Return True if no errors.
            skip:
                List of attributes to skip during validation checks.
        """
        match how:
            case "all":
                errors = defaultdict(list)
                for key, err in self.validation_errors(skip=skip):
                    errors[key].append(err)
                return cast(dict, errors)
            case "eager":
                return next(self.validation_errors(skip=skip), None)
            case "bool":
                return next(self.validation_errors(skip=skip), None) is None
            case "raises":
                errors = self.validate("all", skip=skip)
                if errors:
                    raise ValueError(errors)
            case _:
                raise ValueError(f"invalid method: {how}")

    def validation_errors(self, *, skip=()) -> Iterator[tuple[str, Error]]:
        """
        Yield a sequence of validation errors, if found.

        Each error
        """
        self.update_type_hints(_auto=True)
        for attr, field in self.__dataclass_fields__.items():
            if attr in skip:
                continue

            value = getattr(self, attr)
            if not typecheck(value, field.type):
                yield attr, WrongTypeError(type(value), field.type)


# =============================================================================
# Mixin Classes
# =============================================================================
class ParentMixin:
    """
    Mixin class for models that have a runtime-bound parent property.
    """

    @property
    def parent(self):
        try:
            return self._parent()
        except AttributeError:
            return None

    @parent.setter
    def parent(self, parent):
        self._parent = ref(parent)


class JSONMixin(Protocol):
    """
    Mixin class for models that can be serialized to JSON.
    """

    @classmethod
    def from_dict(cls, data: dict):
        """
        Create a model from a dictionary.
        """
        if "@type" in data:
            data = dict(data)
            del data["@type"]
        return build(data, cls)

    def to_json(self, *, indent=None, sort_keys=False, class_tag=False, **kwargs):
        """
        Return a JSON representation of the model.
        """
        data = self.to_dict()
        return json.dumps(data, indent=indent, sort_keys=sort_keys, **kwargs)

    def to_dict(self):
        """
        Return a dictionary representation of the model.
        """
        try:
            fn = self.__primitive_encode__  # type: ignore
        except AttributeError:
            data = primitive.json(self)
        else:
            data = fn()

        if isinstance(data, dict):
            data.pop("@type", None)
        return data


def resolve_type_hint(
    cls: type,
    attr: str,
    typ: TypeHint,
    globalns: Kwargs = MappingProxyType({}),  # type: ignore
    localns: Kwargs = MappingProxyType({}),  # type: ignore
    recursive_guard: set = frozenset(),  # type: ignore
    raises=False,
):
    """
    Evaluate all forward references in the given type t.
    For use of globalns and localns see the docstring for get_type_hints().
    recursive_guard is used to prevent infinite recursion with a recursive
    ForwardRef.
    """
    if isinstance(typ, str):
        if typ in localns:
            typ = localns[typ]
        elif typ in globalns:
            typ = globalns[typ]

        if isinstance(typ, str):
            typ = ForwardRef(typ)

    if isinstance(typ, (_GenericAlias, GenericAlias, UnionType)):
        try:
            ev_args = tuple(
                resolve_type_hint(
                    cls, attr, arg, globalns, localns, recursive_guard, raises
                )
                for arg in typ.__args__  # type: ignore
            )
        except TypeError:
            raise TypeError(f"cannot resolve: {cls.__name__}.{attr}: {typ}")

        if ev_args == typ.__args__:  # type: ignore
            return typ

        if isinstance(typ, GenericAlias):
            return GenericAlias(typ.__origin__, ev_args)
        elif isinstance(typ, UnionType):
            return functools.reduce(operator.or_, ev_args)
        else:
            return typ.copy_with(ev_args)  # type: ignore

    if isinstance(typ, ForwardRef):
        typ = typ._evaluate(globalns, localns, recursive_guard)  # type: ignore

    if raises and not isinstance(typ, (type, TypeVar)):
        raise TypeError(f"cannot resolve: {cls.__name__}.{attr}: {typ}")

    return typ


_GenericAlias = type(list[int])
