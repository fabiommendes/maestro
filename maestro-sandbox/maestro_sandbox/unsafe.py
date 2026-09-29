from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol, TypeVar

from . import subprocess
from .types import AnyPath, Cmd, Sandbox, cmd_list, cmd_shell

Args = TypeVar("Args", bound=tuple)
Kwargs = TypeVar("Kwargs", bound=dict[str, Any])


NOT_GIVEN = NotImplemented


@dataclass
class Signature[Args: tuple, Kwargs: dict[str, Any]]:
    args: Args
    kwargs: Kwargs


class ACallable[**P, R](Protocol):
    __qualname__: str

    async def __call__(self, /, *args: P.args, **kwargs: P.kwargs) -> R: ...


class Fn[**P, R](Sandbox[Signature, R]):
    """
    Uses the Sandbox protocol to run a Python function unsafely.

    This is useful for testing and debugging purposes only. Do not use in
    production if you don't trust the code being run.

    Example:
        >>> def add(x: int, y: int) -> int:
        ...    return x + y
        >>> sandbox = FnUnsafeSandbox(add)
        >>> result = await sandbox.run(args(1, 2))
        >>> assert result.value == 3
    """

    def __init__(
        self,
        function: Callable[P, R],
        /,
        *,
        cwd: AnyPath | None = None,
        env: dict[str, str] | None = None,
        id: str = NOT_GIVEN,
    ) -> None:
        self.id = function.__qualname__ if id is NOT_GIVEN else id
        self.function = function
        self.cwd = cwd
        self.env = env

    async def _runnner(self, input: Signature) -> R:
        return self.function(*input.args, **input.kwargs)


class AsyncFn[**P, R](Sandbox[Signature, R]):
    """
    FnUnsafeSandbox for async functions.

    Example:
        >>> async def add(x: int, y: int) -> int:
        ...    return x + y
        >>> sandbox = AsyncFnUnsafeSandbox(add)
        >>> result = await sandbox.run(args(1, 2))
        >>> assert result.value == 3
    """

    def __init__(
        self,
        function: ACallable[P, R],
        /,
        *,
        cwd: AnyPath | None = None,
        env: dict[str, str] | None = None,
        id: str = NOT_GIVEN,
    ) -> None:
        self.id = function.__qualname__ if id is NOT_GIVEN else id
        self.function = function
        self.cwd = cwd
        self.env = env

    async def _runner(self, input: Signature) -> R:
        return await self.function(*input.args, **input.kwargs)


class Subprocess(Sandbox[str, int]):
    def __init__(
        self,
        cmd: Cmd,
        /,
        *,
        cwd: AnyPath | None = None,
        env: dict[str, str] | None = None,
        id: str = NOT_GIVEN,
    ) -> None:
        self.cmd = cmd
        self.cwd = cwd
        self.env = env
        if id is NOT_GIVEN:
            if isinstance(cmd, str):
                self.id = cmd
            else:
                self.id = " ".join(cmd)
        self.id = cmd_shell(cmd) if id is NOT_GIVEN else id

    async def _runner(self, input: str) -> int:
        process = subprocess.create_subprocess(
            cmd_list(self.cmd),
            cwd=self.cwd or Path.cwd(),
            env=self.env,
        )

        if input != "":
            # TODO: support input
            raise ValueError("SubprocessUnsafeSandbox does not support input yet")

        result = await subprocess.run_subprocess(process)
        return result.code


def args(*args, **kwargs) -> Signature[tuple[Any, ...], dict[str, Any]]:
    """
    Construct the arguments to pass to FnUnsafeSandbox.run.

    Example:
        >>> def add(x: int, y: int) -> int:
        ...    return x + y
        >>> sandbox = FnUnsafeSandbox(add)
        >>> result = await sandbox.run(args(1, 2))
        >>> assert result.value == 3
    """
    return Signature(args=args, kwargs=kwargs)
