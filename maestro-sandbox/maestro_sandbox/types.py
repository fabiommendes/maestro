import pathlib
import shlex
import time
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

import trio

type AnyPath = pathlib.Path | trio.Path
type Cmd = list[str] | str


class Outcome(str, Enum):
    SUCCESS = "success"
    TIMEOUT = "timeout"
    MEMORY_LIMIT_EXCEEDED = "memory_limit_exceeded"
    RUNTIME_ERROR = "runtime_error"
    INTERNAL_ERROR = "internal_error"


@dataclass
class Result[T]:
    value: T | None
    duration: float
    outcome: Outcome
    error: Exception | None = None
    stdout: str | None = None
    stderr: str | None = None


class Sandbox[In, Out](Protocol):
    id: str
    cwd: AnyPath | None
    env: dict[str, str] | None

    async def prepare(self) -> None: ...

    async def run(self, input: In) -> Result[Out]:
        t0 = time.perf_counter()
        try:
            result = await self._runner(input)
            return Result(
                value=result,
                duration=time.perf_counter() - t0,
                outcome=Outcome.SUCCESS,
            )
        except Exception as e:
            return Result(
                value=None,
                duration=time.perf_counter() - t0,
                outcome=Outcome.INTERNAL_ERROR,
                error=e,
            )

    async def _runner(self, input: In) -> Out: ...


def cmd_shell(cmd: Cmd) -> str:
    """
    Convert a command to a shell command string.

    Args:
        cmd: The command to convert.

    Returns:
        The command as a shell command string.
    """
    if isinstance(cmd, str):
        return cmd
    return " ".join(shlex.quote(arg) for arg in cmd)


def cmd_list(cmd: Cmd) -> list[str]:
    """
    Convert a command to a list of strings.

    Args:
        cmd: The command to convert.

    Returns:
        The command as a list of strings.
    """
    if isinstance(cmd, list):
        return cmd
    import shlex

    return shlex.split(cmd)


def trio_path(path: AnyPath) -> trio.Path:
    """
    Convert a pathlib.Path to a trio.Path.
    """
    if isinstance(path, trio.Path):
        return path
    return trio.Path(path)


def py_path(path: AnyPath) -> pathlib.Path:
    """
    Convert a trio.Path to a pathlib.Path.
    """
    if isinstance(path, pathlib.Path):
        return path
    return pathlib.Path(path)
