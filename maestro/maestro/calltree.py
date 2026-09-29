"""
Represents a chain of single argument function calls.
"""

from __future__ import annotations

import abc
import enum
from dataclasses import dataclass
from functools import partial, wraps
from typing import Callable, Iterable, NamedTuple, Protocol, overload

from .utils import function_name, named_lambda

ERROR = object()
NOT_GIVEN = NotImplemented

type Fn[X, R] = Callable[[X], R]


class pair[A, B](NamedTuple):
    """
    A tuple with two elements, `first` and `second`.
    """

    first: A
    second: B


#
# Utility functions
#
def link[X, R, **P](func: FnExt[X, R, P]) -> Callable[P, CallTree[X, R]]:
    """
    The decorated function is a factory that creates a node in a call tree
    that calls the decorated function with the given additional arguments.

    Example:
        @link
        def add(x: int, /, by: int=1) -> int:
            return x + by

        incr = add(by=1)       # now incr is part of a call tree
        incr.chain(add(by=2))  # creates a chain of two calls

    """
    return lambda *args, **kwargs: Call(partial(func, *args, **kwargs))


def source[R, **P](func: Callable[P, R]) -> Callable[P, CallTree[None, R]]:
    """
    The decorated function is a factory that creates source nodes in a call tree.

    Source nodes do not receive arguments and are used to start a call tree.

    Example:
        @source
        def answer(value: int=42) -> int:
            return value

        root = answer()  # creates a source node in a call tree
    """

    def factory(*args, **kwargs):
        @wraps(func)
        def wrapped(x):
            return func(*args, **kwargs)

        return Call(wrapped)

    return factory


def call[X, R](func: Fn[X, R]) -> CallTree[X, R]:
    """
    Coerce a function/callable into a call tree node.
    """
    if isinstance(func, CallTree):
        return func
    if not callable(func):
        raise TypeError(f"Expected a callable, got {type(func).__name__}")
    return Call(func)


def call_with[X, R, **P](
    func: FnExt[X, R, P], /, *args: P.args, **kwargs: P.kwargs
) -> CallTree[X, R]:
    """
    Coerce a function/callable into a call tree node with additional arguments.
    """
    name = function_name(func)
    fn = named_lambda(name, lambda x: func(x, *args, **kwargs))
    return Call(fn)


def always[S, T](value: T, *, name: str = NOT_GIVEN) -> CallTree[S, T]:
    """
    Create a no-op task that does nothing and return a value fixed.
    """
    if name is not NOT_GIVEN:
        show_as = name
    elif type(value) in (int, float, type(None)):
        show_as = "always-" + repr(value)
    else:
        show_as = "always-" + type(value).__name__.lower()
    return Call(named_lambda(show_as, lambda _: value))


def each[X, Y, Z](
    task: CallTree[X, list[Y]], each_task: CallTree[Y, Z], parallel: bool = False
) -> CallTree[X, list[Z]]:
    """
    Create a task that applies the given function to each item in the task's result.
    """
    return task.chain(Each(task=each_task, parallel=parallel))


@overload
def chain[Xa, Xb, Xc](
    f1: Fn[Xa, Xb],
    f2: Fn[Xb, Xc],
    /,
) -> CallTree[Xa, Xc]: ...


@overload
def chain[Xa, Xb, Xc, Xd](
    f1: Fn[Xa, Xb],
    f2: Fn[Xb, Xc],
    f3: Fn[Xc, Xd],
    /,
) -> CallTree[Xa, Xd]: ...


@overload
def chain[Xa, Xb, Xc, Xd, Xe](
    f1: Fn[Xa, Xb],
    f2: Fn[Xb, Xc],
    f3: Fn[Xc, Xd],
    f4: Fn[Xd, Xe],
    /,
) -> CallTree[Xa, Xe]: ...


@overload
def chain[Xa, Xb, Xc, Xd, Xe, Xf](
    f1: Fn[Xa, Xb],
    f2: Fn[Xb, Xc],
    f3: Fn[Xc, Xd],
    f4: Fn[Xd, Xe],
    f5: Fn[Xe, Xf],
    /,
) -> CallTree[Xa, Xf]: ...


@overload
def chain[Xa, Xb, Xc, Xd, Xe, Xf, Xg](
    f1: Fn[Xa, Xb],
    f2: Fn[Xb, Xc],
    f3: Fn[Xc, Xd],
    f4: Fn[Xd, Xe],
    f5: Fn[Xe, Xf],
    f6: Fn[Xf, Xg],
    /,
) -> CallTree[Xa, Xg]: ...


def chain(*chain):
    """
    Chain functions together.
    """
    first, *rest = map(call, chain)
    task = first
    for next_task in rest:
        task = task.chain(next_task)
    return task


def split[X, R1, R2](
    first: CallTree[X, R1],
    second: CallTree[X, R2],
    /,
    *,
    parallel: bool = False,
) -> Pair[X, R1, R2]:
    """
    Create a pair of tasks that split in two.
    """
    return Pair(first=first, second=second, parallel=parallel)


def expect[X, R](of: Fn[X, R], /) -> CallTree[R, R]:
    """
    Expect a task to return a value of the given type.
    """
    return Call(named_lambda("id", lambda x: x))


def map2[A, X, Y, R](
    fn: Callable[[X, Y], R], x: CallTree[A, X], y: CallTree[A, Y]
) -> CallTree[A, R]:
    """
    Map two tasks using the provided function.
    """
    return Pair(x, y).reduce(fn)


def map3[A, X, Y, Z, R](
    fn: Callable[[X, Y, Z], R],
    x: CallTree[A, X],
    y: CallTree[A, Y],
    z: CallTree[A, Z],
) -> CallTree[A, R]:
    """
    Map 3 tasks using the provided function.
    """

    @wraps(fn)
    def reducer(x: X, tail: tuple[Y, Z]) -> R:
        return fn(x, *tail)

    return Pair(x, Pair(y, z)).reduce(reducer)


def map4[A, X, Y, Z, W, R](
    fn: Callable[[X, Y, Z, W], R],
    x: CallTree[A, X],
    y: CallTree[A, Y],
    z: CallTree[A, Z],
    w: CallTree[A, W],
) -> CallTree[A, R]:
    """
    Map 4 tasks using the provided function.
    """

    @wraps(fn)
    def reducer(x: X, tail: tuple[Y, tuple[Z, W]]) -> R:
        y, (z, w) = tail
        return fn(x, y, z, w)

    return Pair(x, Pair(y, Pair(z, w))).reduce(reducer)


def map5[A, X, Y, Z, W, W_, R](
    fn: Callable[[X, Y, Z, W, W_], R],
    x: CallTree[A, X],
    y: CallTree[A, Y],
    z: CallTree[A, Z],
    w: CallTree[A, W],
    w_: CallTree[A, W_],
) -> CallTree[A, R]:
    """
    Map 4 tasks using the provided function.
    """

    @wraps(fn)
    def reducer(x: X, tail: tuple[Y, tuple[Z, tuple[W, W_]]]) -> R:
        y, (z, (w, w_)) = tail
        return fn(x, y, z, w, w_)

    return Pair(x, Pair(y, Pair(z, Pair(w, w_)))).reduce(reducer)


#
# Call tree AST
#
class CallTree[X, R](abc.ABC):
    """
    Each ask represents a unit of work in the pipeline.

    A task tree is defined in each activity and is run/interpreted by passing
    the activity as the argument and producing some task result.
    """

    name: str

    def __call__(self, arg: X, /) -> R:
        """
        Call the task with the given argument.
        """
        awaitable = self.run(arg)
        while True:
            try:
                msg = awaitable.send(None)
                print(msg)
            except StopIteration as e:
                return e.value

    def __rshift__[S](self, other: CallTree[R, S]) -> CallTree[X, S]:
        return self.chain(other)

    def __and__[S](self, other: CallTree[X, S]) -> CallTree[X, S]:
        return Pair(self, other).reduce(named_lambda("second", lambda _, x: x))

    def __or__[S](self, other: CallTree[X, S]) -> CallTree[X, R]:
        return Pair(self, other).reduce(named_lambda("first", lambda x, _: x))

    @abc.abstractmethod
    async def run(self, arg: X, /) -> R:
        """Run the task with given arguments."""
        raise NotImplementedError("Subclasses must implement this method.")

    def begin(self: CallTree[None, R]) -> R:
        """
        Start a source task.
        """
        return self(None)

    def source(self, value: X) -> CallTree[None, R]:
        """
        Create a source task.
        """
        return always(value).chain(self)

    def chain[S](self, task: Fn[R, S]) -> CallTree[X, S]:
        """
        Chain the current function with the next, passing its result to the
        next one.
        """
        return Chain(self, call(task))

    def map[S](self, func: Fn[R, S]) -> CallTree[X, S]:
        """
        Map the result of the task to a new result using the provided function.
        """
        if isinstance(func, CallTree):
            return Chain(self, func)
        name = function_name(func)
        return Map(named_lambda(name, lambda _, r: func(r)), self, uses_first_arg=False)

    def map_in_out[S](self, func: Callable[[X, R], S]) -> CallTree[X, S]:
        """
        Map the result of the task to a new result using the provided function
        that receives both the argument and the result.
        """
        return Map(func, self)

    def pre_map[Y](self, func: Fn[Y, X]) -> CallTree[Y, R]:
        """
        Apply function to prepare argument.
        """
        return call(func).map(self)

    def map_each[eR, eS](
        self: CallTree[X, list[eR]], func: Fn[eR, eS], *, parallel: bool = False
    ) -> CallTree[X, list[eS]]:
        """
        Map the result of the task to a new result using the provided function

        for each element in the list.
        """
        each = Each(call(func), parallel=parallel)
        return self >> each

    def forward_arg(self) -> CallTree[X, X]:
        """
        Forward the argument to the next task without changing it.
        """
        arg: CallTree[X, X] = call(lambda x: x)
        return Pair(arg, self).reduce(named_lambda("fwd_argument", lambda x, _: x))

    def nullable(self) -> CallTree[X | None, R | None]:
        """
        Return a task that may receive None as an argument and return None as
        a result.
        """
        return Either(lambda x: x is not None, self, always(None))  # type: ignore

    def pretty(self) -> str:
        """
        Return a pretty representation of the task tree.
        """
        lines = ["pipeline {", *self._pretty_lines(indent=1), "}"]
        return "\n".join(lines)

    def _pretty_lines(self: CallTree, indent: int = 0) -> Iterable[str]:
        indent_str = "  " * indent
        match self:
            case Chain(first, second):
                yield from first._pretty_lines(indent)
                yield from second._pretty_lines(indent)
            case Each(task, _):
                yield f"{indent_str}for_each {{"
                yield from task._pretty_lines(indent + 1)
                yield indent_str + "}"
            case _:
                yield f"{indent_str}* {self.name}"


@dataclass
class Call[X, R](CallTree[X, R]):
    """
    Represents a sequential task that runs a series of tasks in order.
    """

    func: Fn[X, R]

    @property
    def name(self) -> str:  # type: ignore
        return function_name(self.func, dashes=True)

    async def run(self, arg: X) -> R:
        id = await enter(self, arg)
        return await exit(id, self.func(arg))


@dataclass
class Map[X, S, R](CallTree[X, R]):
    """
    Represents a sequential task that runs a series of tasks in order.
    """

    func: Callable[[X, S], R]
    tree: CallTree[X, S]
    uses_first_arg: bool = True

    @property
    def name(self) -> str:  # type: ignore
        fname = function_name(self.func, dashes=True)
        return f"{fname}({self.tree.name})"

    async def run(self, x: X, /) -> R:
        id = await enter(self, x)
        s = await self.tree.run(x)
        return await exit(id, self.func(x, s))


@dataclass
class Either[X, R1, R2](CallTree[X, R1 | R2]):
    """
    Represents a task that can return either of two results.
    """

    predicate: Fn[X, bool]
    when_true: CallTree[X, R1]
    when_false: CallTree[X, R2]

    @property
    def name(self) -> str:  # type: ignore
        return f"if {function_name(self.predicate, dashes=True)} then {self.when_true.name} else {self.when_false.name}"

    async def run(self, x: X, /) -> R1 | R2:
        id = await enter(self, x)
        r: R1 | R2
        if self.predicate(x):
            r = await self.when_true.run(x)
        else:
            r = await self.when_false.run(x)
        return await exit(id, r)


@dataclass
class Chain[X, Y, R](CallTree[X, R]):
    """
    Represents a chain of tasks that are run in order.
    """

    first: CallTree[X, Y]
    second: CallTree[Y, R]

    @property
    def name(self) -> str:  # type: ignore
        return f"{self.first.name} -> {self.second.name}"

    async def run(self, x: X, /) -> R:
        id = await enter(self, x)
        y = await self.first.run(x)
        r = await self.second.run(y)
        return await exit(id, r)

    def map[S](self, func: Fn[R, S]) -> Chain[X, Y, S]:
        return Chain(first=self.first, second=self.second.map(func))


@dataclass
class Pair[X, R1, R2](CallTree[X, pair[R1, R2]]):
    """
    A pair of tasks from a single argument.
    """

    first: CallTree[X, R1]
    second: CallTree[X, R2]
    parallel: bool = False

    @property
    def name(self) -> str:  # type: ignore
        return f"({self.first.name}, {self.second.name})"

    async def run(self, x: X, /) -> pair[R1, R2]:
        id = await enter(self, x)
        r1 = await self.first.run(x)
        r2 = await self.second.run(x)
        return await exit(id, pair(r1, r2))

    def _copy[Y, S1, S2](
        self, first: CallTree[Y, S1], second: CallTree[Y, S2]
    ) -> Pair[Y, S1, S2]:
        """
        Create a copy of the pair with new tasks.
        """
        return Pair(first=first, second=second, parallel=self.parallel)

    def chain_first[S](self, task: CallTree[R1, S]) -> Pair[X, S, R2]:
        """
        Chain the first task with the given task.
        """
        return self._copy(self.first.chain(task), self.second)

    def chain_second[S](self, task: CallTree[R2, S]) -> Pair[X, R1, S]:
        """
        Chain the first task with the given task.
        """
        return self._copy(self.first, self.second.chain(task))

    def map_first[S](self, func: Fn[R1, S]) -> Pair[X, S, R2]:
        """
        Create a new pair mapping only the first element in the pair.
        """
        return self._copy(self.first.map(func), self.second)

    def map_second[S](self, func: Fn[R2, S]) -> Pair[X, R1, S]:
        """
        Create a new pair mapping only the second element in the pair.
        """
        return self._copy(self.first, self.second.map(func))

    def map_both[S1, S2](
        self,
        func1: Fn[R1, S1],
        func2: Fn[R2, S2],
    ) -> Pair[X, S1, S2]:
        """
        Create a new pair mapping both elements in the pair.
        """
        return self._copy(self.first.map(func1), self.second.map(func2))

    def reduce[R](self, func: Callable[[R1, R2], R]) -> CallTree[X, R]:
        """
        Reduce the results of the two tasks using the provided function.
        """
        return self.map(named_lambda("join", lambda pair: func(*pair)))


@dataclass
class Each[X, R](CallTree[list[X], list[R]]):
    """
    Represents a parallel task that runs multiple tasks, possibly concurrently.
    """

    task: CallTree[X, R]
    parallel: bool = False

    @property
    def name(self) -> str:  # type: ignore
        return f"each {self.task.name}"

    async def run(self, xs: list[X]) -> list[R]:
        id = await enter(self, xs)
        if isinstance(self.task, Chain):
            ss = await Each(self.task.first, self.parallel).run(xs)
            rs = await Each(self.task.second, self.parallel).run(ss)
        else:
            rs = [(await self.task.run(elem)) for elem in xs]
        return await exit(id, rs)

    # def map_each[S](self, func: Callable[[R], S]) -> Each[X, S]:
    #     return Each(task=self.task.map(func), parallel=self.parallel)


class Event(enum.Enum):
    """
    Represents an event that can be used to notify about task execution.
    """

    START = "start"
    END = "end"
    ERROR = "error"


class Id(int):
    """
    Represents a unique identifier for a task.
    """

    counter = 0


def new_id() -> Id:
    """
    Generate a new unique identifier for a task.
    """
    Id.counter += 1
    return Id(Id.counter)


async def enter[X, R](task: CallTree[X, R], arg: X) -> Id:
    """
    Notify that the task is starting.
    """
    return new_id()


async def exit[T](id: Id, value: T) -> T:
    """
    Notify that the task has ended.
    """
    return value


#
# Utility types
#
class CallError(Exception):
    """
    Represents an error that occurred during task execution.

    This is a custom exception to differentiate task errors from other exceptions.
    """

    error: Exception | None

    def __init__(self, error: Exception):
        super().__init__()
        self.error = error


class FnExt[X, R, **P](Protocol):
    """
    Task function with extended arguments
    """

    def __call__(self, arg: X, /, *args: P.args, **kwargs: P.kwargs) -> R: ...


if __name__ == "__main__":

    @link
    def add(x: int, /, by: int = 1) -> int:
        return x + by

    @link
    def mul(x: int, /, by: int = 1) -> int:
        return x * by

    pipeline = add(by=1).chain(mul(by=2))

    print(pipeline.pretty())
    print(pipeline(1))
