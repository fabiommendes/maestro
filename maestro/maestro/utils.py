import io
import os
import re
import sys
import tokenize
from collections import defaultdict
from functools import lru_cache, partial
from pathlib import Path
from types import FunctionType, ModuleType
from typing import Any, Callable, Iterable, Literal, overload

import rich
from cachetools import TTLCache, cached

from .errors import ConfigurationError


def split_camel_case(src: str) -> list[str]:
    """
    Split a camelCase string into a list of words.

    Args:
        src (str): The camelCase string to split.

    Example:
        >>> split_camel_case("camelCaseString")
        ['camel', 'case', 'string']
    """
    return [st.lower() for st in re.findall(r"[a-z]+|[A-Z]+[a-z0-9_]+", src)]


def camel_case_to_snake_case(src: str, sep: str = "_") -> str:
    """
    Convert a camelCase string to snake_case.

    Args:
        src (str): The camelCase string to convert.

    Example:
        >>> camel_case_to_snake_case("camelCaseString")
        'camel_case_string'

        It is possible to use a different separator by providing the `sep`
        argument.
        >>> camel_case_to_snake_case("camelCaseString", sep="-")
        'camel-case-string'
    """
    return sep.join(split_camel_case(src))


def slugify(src: str) -> str:
    """
    Convert a string to a slug format.

    Args:
        src (str): The string to convert.

    Example:
        >>> slugify("Hello World!")
        'hello-world'
    """
    return re.sub(r"[^\w\s-]", "", src).strip().lower().replace(" ", "-")


def numeric_key(n: int, total: int) -> str:
    """
    Generate a numeric key for a given number and total count.

    Args:
        n (int): The current number.
        total (int): The total count.

    Example:
        >>> numeric_key(1, 10)
        '01'
        >>> numeric_key(10, 100)
        '010'
    """
    size = len(str(total))
    data = str(n)
    return data.zfill(size) if size > 1 else data


def is_empty(iterable: Iterable) -> bool:
    """
    Check if an iterable is empty.

    Args:
        iterable: The iterable to check.

    Example:
        >>> is_empty([])
        True
        >>> is_empty([1, 2, 3])
        False
    """
    for _ in iterable:
        return False
    return True


def function_name(func: Callable, dashes: bool = False) -> str:
    """
    Get the name of the function, replacing underscores with hyphens.
    """
    if dashes:
        return function_name(func).replace("_", "-")

    try:
        return func.__name__
    except AttributeError:
        pass

    if isinstance(func, partial):
        return function_name(func.func)

    return str(func).removeprefix("(").removesuffix(")")


def named_lambda[R, **P](name: str, func: Callable[P, R]) -> Callable[P, R]:
    """
    Rename a lamda or other python function.
    """
    if func.__name__ == "<lambda>":
        func.__name__ = name
        return func

    code = func.__code__
    renamed = FunctionType(
        code,
        func.__globals__,
        name,
        argdefs=func.__defaults__,
        closure=func.__closure__,
        kwdefaults=func.__kwdefaults__,
    )
    return renamed  # type: ignore


def indent(text: str, indent: str | int = 4, first: bool = True) -> str:
    """
    Indent a block of text with a specified number of spaces.

    Args:
        text (str): The text to indent.
        spaces (int): The number of spaces or prefix to use for indentation.
        first (bool): Whether to indent the first line.

    Example:
        >>> indent("Hello\nWorld", 2)
        '  Hello\n  World'
    """
    if isinstance(indent, int):
        indent = " " * indent
    if first:
        return "\n".join(indent + line for line in text.splitlines())
    return f"\n{indent}".join(text.splitlines())


def group_duplicated[K, V](
    data: dict[K, V], keep_unique: bool = False
) -> dict[tuple[K, ...], V]:
    """
    Group duplicate items in dictionary and remove the unique ones.

    Return a dictionary mapping tuples of keys to the duplicated values.

    Args:
        data:
            A dictionary mapping keys to values.
        keep_unique:
            If True, it will keep unique matches in the result. Otherwise, it
            will only return duplicates.
    """

    def get_key(ks: set[K]) -> tuple[K, ...]:
        try:
            return tuple(sorted(ks))  # type: ignore
        except TypeError:
            return tuple(sorted(ks, key=str))

    duplicates: dict[V, set[K]] = {v: set() for v in data.values()}
    for k, v in data.items():
        duplicates[v].add(k)

    if not keep_unique:
        duplicates = {v: ks for v, ks in duplicates.items() if len(ks) > 1}
    return {get_key(ks): v for v, ks in duplicates.items()}


@cached(cache=TTLCache(maxsize=256, ttl=60))
def existing_folder(path: Path) -> Path:
    """
    Return a folder path and create it if it does not exist.

    Args:
        path (Path): The path to check.

    Raises:
        ValueError: If path points to a file instead of a directory.
    """
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    return path


def merge_dicts[V, K = str, Id = str](db: dict[Id, dict[K, V]]) -> dict[K, V]:
    """
    Merge dict of dicts
    """
    merged: dict[K, V] = {}
    sources: dict[K, Id] = {}

    for key, mapping in db.items():
        for k, v in mapping.items():
            if k in merged:
                msg = f"Duplicate key found: {k!r} in {key!r} and {sources[k]!r}."
                raise ValueError(msg)
            merged[k] = v
            sources[k] = key
    return merged


def merge_dicts_of_dicts[A, B, C](
    d1: dict[A, dict[B, C]], d2: dict[A, dict[B, C]]
) -> dict[A, dict[B, C]]:
    """
    Merge two dictionaries of dictionaries.
    """
    merged: dict[A, dict[B, C]] = defaultdict(dict)
    for key in set(d1) | set(d2):
        merged[key].update(d1.get(key, {}))
        merged[key].update(d2.get(key, {}))
    return merged


def transpose_dicts[A, B, C](data: dict[A, dict[B, C]]) -> dict[B, dict[A, C]]:
    """
    Transpose a dictionary of dictionaries.

    Args:
        data:
            A dictionary where keys are of type A and values are dictionaries
            with keys of type B and values of type C.

    Returns:
        A transposed dictionary where keys are of type B and values are
        dictionaries with keys of type A and values of type C.
    """
    out: dict[B, dict[A, C]] = {}
    for a_key, b_dict in data.items():
        for b_key, value in b_dict.items():
            out.setdefault(b_key, {})[a_key] = value
    return out


def is_entry_point_name(
    name: str,
    is_name: Callable[[str], bool] = re.compile(tokenize.Name).match,  # type: ignore
) -> bool:
    """
    Check if the given name is a valid Python qualified name.
    """
    mod, sep, func_name = name.partition(":")
    if not sep:
        return False
    parts = mod.split(".")
    return bool(parts) and all(map(is_name, parts)) and bool(is_name(func_name))


@overload
def load_entry_point(
    name: str, base: Path = Path("."), *, check_callable: Literal[True]
) -> Callable[..., Any]: ...


@overload
def load_entry_point(
    name: str, base: Path = Path("."), *, check_callable: bool = False
) -> Any: ...


@lru_cache(maxsize=256)
def load_entry_point(name, base=Path("."), *, check_callable=False) -> Any:
    """
    Load a python object from an entry point string specification.
    """
    if not is_entry_point_name(name):
        msg = f"Invalid entry point name: {name!r}. Must be in the form 'mod:obj'."
        raise ValueError(msg)

    mod_name, _, obj_name = name.partition(":")
    mod_file = mod_name.replace(".", os.sep) + ".py"
    mod_name = "maestro.script." + mod_name
    mod_path = base.resolve() / mod_file

    if not mod_path.exists():
        raise ConfigurationError(f"Module {mod_name!r} not found at {str(mod_path)!r}.")

    # TODO: Use importlib instead of exec
    # spec = importlib.util.spec_from_file_location(name, script_path)
    # module = importlib.util.module_from_spec(spec)
    # spec.loader.exec_module(module)

    source = mod_path.read_text(encoding="utf-8")
    code = compile(source, mod_path, "exec", dont_inherit=True)
    sys.modules[mod_name] = mod = mod = ModuleType(mod_name, "")

    mod.__dict__["__file__"] = str(mod_path)
    exec(code, mod.__dict__)

    try:
        obj = getattr(mod, obj_name)
    except AttributeError:
        msg = f"Name {obj_name!r} is not present in module {mod_name!r}."
        raise ConfigurationError(msg)
    finally:
        del sys.modules[mod_name]

    if check_callable and not callable(obj):
        msg = f"Object {name!r} is not callable, got: {obj.__class__.__name__}."
        raise ConfigurationError(msg)

    return obj


def normalize_text(text: str) -> str:
    """
    Normalize text by removing extra spaces and newlines.
    """
    return " ".join(text.split()).strip().lower()


def rich_render(obj: Any) -> str:
    """
    Render an object to a string using rich.
    """
    fd = io.StringIO()
    rich.print(obj, end="", file=fd)
    return fd.getvalue()


def max_non_null(*args):
    """
    Return the maximum value from the arguments, ignoring None values.
    """
    return max(x for x in args if x is not None)


def min_non_null(*args):
    """
    Return the minimum value from the arguments, ignoring None values.
    """
    return min(x for x in args if x is not None)


def map_dicts(f, *dicts: dict, fill=None):
    """
    Map a function over multiple dictionaries, merging the results.

    Example:
        >>> a = {'x': 1, 'y': 2}
        >>> b = {'x': 3, 'y': 4, "z": 5}
        >>> map_dicts(lambda x, y: x + y, a, b, fill=0)
        {'x': 4, 'y': 6, 'z': 5}
    """
    if not dicts:
        return {}

    first, *tail = [d.copy() for d in dicts]
    result = {}
    empty_tail = False
    while not empty_tail:
        while first:
            key, value = first.popitem()
            values = [value]
            for other in tail:
                values.append(other.pop(key, fill))
            result[key] = f(*values)

        empty_tail = True
        for d in tail:
            if d:
                key = next(iter(d))
                first[key] = fill
                empty_tail = False
                break

    return result
