import shlex
import shutil
import subprocess
from collections import deque
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Callable

import trio

from .errors import ExecutionError
from .types import AnyPath, Cmd, cmd_list, cmd_shell


@dataclass
class Subprocess:
    """
    Represents a subprocess command to be executed.
    """

    cmd: list[str]
    cwd: AnyPath = Path(".")
    env: dict[str, str] | None = None


@dataclass
class Execution:
    """
    Represents the results of running a command.
    """

    cmd: Cmd
    code: int
    stdout: StringIO

    @property
    def args(self) -> list[str]:
        """
        Get the command arguments as a list of strings.
        """
        return cmd_list(self.cmd)

    @property
    def cmd_string(self) -> str:
        """
        Get the command as a shell command string.
        """
        return cmd_shell(self.cmd)

    def __str__(self) -> str:
        return self.cmd_string

    def read(self) -> str:
        """
        Read the output of the command.

        Returns:
            The output of the command as a string.
        """
        return self.stdout.getvalue()

    def format(self) -> str:
        """
        Format the execution result as a string.

        Returns:
            A formatted string with the command, code, and output.
        """
        lines = [
            f"[b]$[yellow]{self.cmd_string}[/][/]",
            f"[b]Code[/b]: {self.code}",
            "[b]Output[/b]:",
        ]
        for line in self.read().splitlines():
            lines.append(f"  {line}")
        return "\n".join(lines)


async def run_subprocess(
    pargs: Subprocess,
    on_line: Callable[[str], None] | None = print,
    valid_codes: list[int] | None = None,
    check: bool = True,
) -> Execution:
    """
    Execute the subprocess command associated with this action.

    Args:
        process:
            The subprocess.Popen object representing the running process.
        on_line:
            A callback function that is called for each line of output from the process.
    """
    line: str
    buf = StringIO()

    def push_line(line: str):
        buf.write(line + "\n")
        if on_line is not None:
            on_line(line)

    process: trio.Process = await trio.lowlevel.open_process(
        pargs.cmd,
        cwd=pargs.cwd,
        env=pargs.env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert process.stdout is not None, "Process must have stdout captured."

    # Interact with the process stdout
    lines: deque[str] = deque([])
    while raw := await process.stdout.receive_some():
        text = raw.decode("utf-8")
        prefix = lines.popleft() if lines else ""
        for line in text.splitlines(keepends=True):
            lines.append(line)
        lines[0] = prefix + lines[0]

        while lines and lines[0].endswith("\n"):
            push_line(lines.popleft().removesuffix("\n").removesuffix("\r"))
    if lines:
        push_line(lines.popleft())

    code = await process.wait()
    cmd = quote_args(process.args)
    if valid_codes is not None and code not in valid_codes:
        raise ExecutionError(cmd=cmd, code=code, stdout=buf)
    else:
        return Execution(cmd=cmd, code=code, stdout=buf)


def create_subprocess(
    cmd: list[str],
    cwd: AnyPath = Path("."),
    env: dict[str, str] | None = None,
) -> Subprocess:
    """
    Create a subprocess with the given command, current working directory,
    """

    executable = shutil.which(cmd[0])
    if executable is None:
        raise RuntimeError(f"Executable '{cmd[0]}' not found in PATH.")
    cmd[0] = executable
    cwd.mkdir(parents=True, exist_ok=True)
    return Subprocess(cmd, cwd=cwd, env=env)


def quote_args(args) -> str:
    """
    Render a list of command-line arguments into a single string.

    Args:
        args:
            The list of command-line arguments.

    Returns:
        A string representation of the command-line arguments.
    """
    if isinstance(args, str):
        return args
    elif isinstance(args, bytes):
        return args.decode("utf-8")
    elif isinstance(args, Path):
        return str(args)
    elif isinstance(args, list):
        return " ".join(shlex.quote(arg) for arg in args)
    raise TypeError(f"invalid process args: {args.__class__.__name__}")
