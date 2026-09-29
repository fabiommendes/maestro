"""
Fred utilities and encoders/decoders shared between all hey-teach modules.
"""

import collections
import datetime
from functools import wraps
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, Mapping, Sequence, TypeVar, overload

import fred
from fred import Tag

T = TypeVar("T")
Meta = Dict[str, Any]
TagHandler = Callable[[str, Meta, Any], Any]
TAG_CONFIG_CONSTRUCTORS: Dict[str, TagHandler] = {}


class Fred:
    """
    FRED data serialization and deserialization.
    """

    @property
    def casefold(self) -> bool:
        "True if tags names are casefolded."
        return self._casefold

    @property
    def tag_hooks(self) -> Mapping[str, TagHandler]:
        "Read-only access to the tag handlers."
        return MappingProxyType(self._tag_hooks)

    def __init__(self, *, missing_tag="expand", casefold=False):
        self._tag_hooks = tag_hooks = {}
        self._casefold = casefold

        # Register tag hooks
        attr = f"_make_tag_hook_{missing_tag}"
        if hasattr(self, attr):
            fetch = (
                (lambda k: tag_hooks[k.casefold()])
                if casefold
                else tag_hooks.__getitem__
            )
            self._tag_hook = getattr(self, attr)(fetch)
        else:
            raise ValueError(f"unknown tag hook: {missing_tag}")

    def loads(self, s: str, **kwargs) -> Any:
        """
        Load FRED data from string.
        """
        kwargs.setdefault("tag_hook", self._tag_hook)
        return fred.loads(s, **kwargs)

    def load(self, fd, **kwargs) -> Any:
        """
        Load FRED data from a file-like object.
        """
        kwargs.setdefault("tag_hook", self._tag_hook)
        return fred.load(fd, **kwargs)

    def dump(self, obj, fd=None, **kwargs):
        """
        Serialize ``obj`` to a FRED formatted stream using ``fd``'s write method.
        """
        return fred.dump(obj, fd, **kwargs)

    def dumps(self, obj, **kwargs):
        """
        Serialize ``obj`` to a FRED formatted stream using ``fd``'s write method.
        """
        return fred.dumps(obj, **kwargs)

    #
    # Hook factories
    #
    def _make_tag_hook_expand(self, fetch) -> TagHandler:
        def hook(tag: str, meta: dict, data: dict) -> Any:
            try:
                obj = fetch(tag)(tag, meta, data)
            except KeyError:
                items: Iterable[tuple[str, Any]]
                try:
                    items = data.items()
                except AttributeError:
                    items = [("data", data)]
                obj = {"tag": tag, "meta": meta}
                obj.update(items)
            return obj

        return hook

    def _make_tag_hook_clean(self, fetch) -> TagHandler:
        def hook(tag: str, meta: dict, data: dict) -> Any:
            try:
                obj = fetch(tag)(tag, meta, data)
            except KeyError:
                obj = data
            return obj

        return hook

    def _make_tag_hook_wrap(self, fetch) -> TagHandler:
        def hook(tag: str, meta: dict, data: dict) -> Any:
            try:
                obj = fetch(tag)(tag, meta, data)
            except KeyError:
                obj = {"tag": tag, "meta": meta, "data": data}
            return obj

        return hook

    def _make_tag_hook_error(self, fetch) -> TagHandler:
        def hook(tag: str, meta: dict, data: dict) -> Any:
            try:
                obj = fetch(tag)(tag, meta, data)
            except KeyError:
                raise ValueError("unknown tag: %s" % tag)
            return obj

        return hook

    @overload
    def tag_hook(self, /, tag: str, hook: TagHandler) -> None:
        ...

    @overload
    def tag_hook(self, /, tag: str) -> Callable[[TagHandler], TagHandler]:
        ...

    def tag_hook(self, /, tag, hook=None):
        """
        Register a constructor for a FRED tag.
        """
        if hook is None:

            def decorator(fn: TagHandler, /) -> TagHandler:
                self.tag_hook(tag, fn)
                return fn

            return decorator

        if self._casefold:
            tag = tag.casefold()
        self._tag_hooks[tag] = hook


def dataclass_hook(*args, **fields):
    """
    Create a tag hook for a dataclass.
    """
    match args:
        case (cls,):
            construct = lambda obj: cls(**obj)
        case (cls, construct):
            pass
        case _:
            raise TypeError("dataclass_hook() takes 1 or 2 arguments")

    field_set = set(cls.__dataclass_fields__)
    if construct is None:
        construct = lambda obj: cls(**obj)
    if bad := fields.keys() - field_set:
        raise ValueError(f"invalid fields: {bad}")

    def encoder(_tag, _meta, data):
        if isinstance(data, cls):
            return data

        if not isinstance(data, dict):
            raise TypeError(f"expect a mapping, got: {type(data)}")

        if bad := data.keys() - field_set:
            raise ValueError(f"invalid fields: {bad}")

        for k, fn in fields.items():
            if k in data:
                data[k] = fn(data[k])

        return construct(data)

    return encoder


class FredTransformer:
    """
    Transformer class for FRED data.

    Instances of this class work as callables that receive a fred object
    and return it transformed.
    """

    _atomic_types = (
        *[int, float, str, bytes, bool, type(None)],
        *[datetime.date, datetime.time, datetime.datetime],
    )
    _sequence_types = (list, tuple, set, frozenset)
    _mapping_types = (dict, collections.OrderedDict)

    def __init__(self, *, casefold=False, strict=False):
        self._casefold = casefold
        self._strict = strict

    def __tag_hook__(self, tag: str, meta: dict, data: Any):
        """
        Fallback method for constructing tags.

        The default behavior is to select the method based on the tag name
        to handle tags. If no such method exists, the tag hook is used.

        The default implementation simply constructs a tag object from
        its transformed parts.
        """
        return Tag.new(tag, meta, data)

    def __mapping_hook__(self, items, kind):
        """
        Convert an iterator of (key, value) pairs into a mapping.
        """
        return kind(items)

    def __sequence_hook__(self, elems, kind):
        """
        Convert an iterator over elements into a sequence.

        The second argument corresponds to the original sequence
        type. Implementors may choose to keep it or override it.
        """
        return kind(elems)

    def __object_hook__(self, obj):
        """
        Called for unknown types.
        """
        return obj

    def __call__(self, obj):
        """
        Transform Fred data structure by processing it from leaves to root.
        """
        if isinstance(obj, Tag):
            tag = obj.tag.casefold() if self._casefold else obj.tag

            factory: TagHandler
            try:
                factory = getattr(self, tag)
                factory = getattr(factory, "__fred_factory__", factory)
            except AttributeError:
                if self._strict:
                    raise ValueError(f"no handler for tag {tag!r}")
                factory = self.__tag_hook__

            attrs = obj.attrs
            value = obj.value
            if getattr(factory, "__fred_visit__", True):
                attrs = self(attrs)
                value = self(value)
            return factory(tag, attrs, value)

        elif isinstance(obj, self._atomic_types):
            return obj

        elif isinstance(obj, self._mapping_types):
            atomic = self._atomic_types
            check = isinstance
            pairs = ((k, v if check(v, atomic) else self(v)) for k, v in obj.items())
            return self.__mapping_hook__(pairs, type(obj))

        elif isinstance(obj, self._sequence_types):
            atomic = self._atomic_types
            check = isinstance
            seq = (v if check(v, atomic) else self(v) for v in obj)
            cls = type(obj)
            return self.__sequence_hook__(seq, cls)

        elif self._strict:
            raise TypeError("unsupported type: %s" % type(obj))

        else:
            return self.__object_hook__(obj)


def role(inline=False, visit=False):
    """
    A decorator that changes the behavior of visitor methods.

    @role(inline=True):
        Splice dict or list values when calling the corresponding
        method. Atomic values are passed as the single argument
        to the method.

    @role(visit=False):
        Prevents transform children before passing them to the
        constructor.
    """

    def decorator(fn):
        if inline:

            @wraps(fn)
            def factory(self, tag, meta, data):
                if isinstance(data, Mapping):
                    return fn(self, **data)
                # prevent str being treated as a sequence
                elif isinstance(data, (str, bytes)):
                    pass
                elif isinstance(data, Sequence):
                    return fn(self, *data)
                return fn(data)

            fn.__fred_factory__ = factory
        fn.__fred_visit__ = visit
        return fn

    return decorator


#
# Global API
#
fred_api = Fred()
loads = fred_api.loads
load = fred_api.load
dump = fred_api.dump
dumps = fred_api.dumps
tag_encoder = fred_api.tag_hook
