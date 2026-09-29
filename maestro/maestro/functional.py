from typing import Callable, Iterable

from returns.result import Result, Success


def id[T](x: T) -> T:
    """
    Identity function that returns the input value unchanged.
    """
    return x


def always[T](value: T) -> Callable[..., T]:
    """
    Returns a function that always returns the given value.
    """
    return lambda *args, **kwargs: value


def filter_results[V, E](results: Iterable[Result[V, E]]) -> Iterable[V]:
    """
    Remove all non-successful results from the iterable.
    """
    yield from (result.unwrap() for result in results if isinstance(result, Success))


def filter_result_list[V, E](results: list[Result[V, E]]) -> list[V]:
    """
    Remove all non-successful results from the list.
    """
    return [result.unwrap() for result in results if isinstance(result, Success)]
