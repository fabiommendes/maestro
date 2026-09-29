from dataclasses import Field
from typing import Iterable, Any, Callable, cast, get_type_hints
from functools import singledispatch
import copy

from .normalizer import TypeHint, normalize

Kwargs = dict[str, Any]
Builder = Callable[[Kwargs, TypeHint], Any]


class BuildError(TypeError):
    """
    Raised when a builder fails to build an object for some type.
    """


class BuilderT:
    """
    Builds object from mapping, making coercions, when necessary.
    """

    def __init__(self, normalize=normalize):
        @singledispatch
        def build_dispatch(cls, mapping):
            return self._fallback(mapping, cls)

        self._dispatch = build_dispatch.dispatch
        self._register = build_dispatch.register
        self._normalize = normalize

        # Register special builders
        for attr in dir(self):
            if attr.startswith("_build_"):
                fn = getattr(self, attr)
                if fn is None:
                    continue
                try:
                    cls = get_type_hints(fn)["cls"]
                except KeyError:
                    raise NotImplementedError(f'Missing "cls" type hint in {attr}')
                self._register(cls, fn)
                setattr(self, attr, None)

    def __call__(self, mapping: Kwargs, cls: TypeHint) -> Any:
        impl = self._dispatch(cls)
        return impl(cls, mapping)

    def _fallback(self, mapping: Kwargs, hint: TypeHint) -> Any:
        if hasattr(hint, "__dataclass_fields__"):
            cls = cast(type, hint)
            builder = self._dataclass_builder(cls)
            self._register(cls, builder)
            return builder(cls, mapping)
        raise NotImplementedError(hint)

    def _dataclass_builder(self, cls: type) -> Callable:
        sig = {f.name: f.type for f in get_dataclass_fields(cls) if f.init}
        return lambda cls, args: cls(**coerce_args(args, sig, self, self._normalize))


build: Builder = BuilderT()


#
# Auxiliary functions
#
def coerce_args(args: Kwargs, sig: Kwargs, build=None, normalize=normalize) -> Kwargs:
    """
    Coerce arguments to the expected types.

    WARNING: it modifies the args input *in-place* and returns it back.
    """
    for name, value in args.items():
        try:
            cls = sig[name]
        except KeyError:
            continue
        try:
            args[name] = normalize(value, cls)
        except (TypeError, ValueError):
            pass

        if build is not None and isinstance(value, dict):
            try:
                args[name] = build(value, cls)
            except BuildError:
                pass

    return args


def get_dataclass_fields(cls) -> Iterable[Field]:
    annotations = get_type_hints(cls)

    for name, field in cls.__dataclass_fields__.items():
        assert name == field.name, (name, field.name)
        typ = normalize_field_type(field, annotations.get(name), cls)
        if typ is field.type:
            yield field
        else:
            field = copy.copy(field)
            field.type = typ
            yield field


def normalize_field_type(field: Field, annotation, cls) -> TypeHint:
    if field.type in (None, Any):
        return None
    return annotation or field.type
